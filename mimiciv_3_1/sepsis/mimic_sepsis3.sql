-- =====================================================================
-- File: mimic_sepsis3.sql
-- BigQuery Standard SQL
-- Output: <PROJECT>.<DATASET>.sepsis3
--
-- Purpose
--   Sepsis-3 within ICU using Day-1 worst SOFA (first_day_sofa).
--   SOFA total is rebuilt by replacing only the cardiovascular subscore
--   with our custom rules (aligned with aSOFA/SOFA-CV scripts), while
--   keeping respiration/coagulation/liver/CNS/renal from first_day_sofa.
--   CNS IS INCLUDED in the total SOFA (per Sepsis-3 definition).
--
-- Consistency note
--   For cross-database consistency with eICU, this final table now *also*
--   includes two convenience flags:
--     - septic_on_admission  : suspected_infection_time <= ICU intime
--     - sepsis_within_24h    : ICU intime < suspected_infection_time <= ICU intime + 24h
--   These are derived at the very end from ICU admission time.
--
-- Inputs
--   physionet-data.mimiciv_3_1_icu.icustays
--   physionet-data.mimiciv_3_1_hosp.patients
--   physionet-data.mimiciv_3_1_icu.inputevents
--   physionet-data.mimiciv_3_1_icu.chartevents
--   physionet-data.mimiciv_3_1_derived.first_day_sofa
--   <PROJECT>.<DATASET>.suspicion_of_infection
--
-- Output columns
--   subject_id | stay_id | hadm_id | ab_id | antibiotic
--   suspected_infection_time | antibiotic_time | culture_time
--   sofa_time | sofa_score | respiration | coagulation | liver | cardiovascular | cns | renal
--   sepsis3 | septic_on_admission | sepsis_within_24h
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.sepsis3 AS
WITH
-- Adults (>=18) & FIRST ICU stay per subject
first_icu AS (
  SELECT
    i.subject_id, i.stay_id, i.hadm_id, i.intime, i.outtime,
    ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime) AS rn
  FROM `physionet-data.mimiciv_3_1_icu.icustays` i
  JOIN `physionet-data.mimiciv_3_1_hosp.patients` p
    ON p.subject_id = i.subject_id
  WHERE p.anchor_age >= 18
),
first_stay AS (
  SELECT
    subject_id, stay_id, hadm_id, intime, outtime,
    DATETIME_ADD(intime, INTERVAL 24 HOUR) AS intime_24h
  FROM first_icu
  WHERE rn = 1
),

-- Day-1 SOFA subscores (worst-of-day) from first_day_sofa
day1_sofa AS (
  SELECT
    fs.subject_id,
    fs.stay_id,
    COALESCE(fd.respiration, 0) AS respiration,
    COALESCE(fd.coagulation, 0) AS coagulation,
    COALESCE(fd.liver, 0)       AS liver,
    COALESCE(fd.cns, 0)         AS cns,
    COALESCE(fd.renal, 0)       AS renal
  FROM first_stay fs
  LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_sofa` fd
    ON fd.stay_id = fs.stay_id
),

-- Vasopressor dictionary
vasopressor_ids AS (
  SELECT * FROM UNNEST([
    STRUCT(221906 AS itemid, 'norepinephrine' AS label),
    STRUCT(221289 AS itemid, 'epinephrine'     AS label),
    STRUCT(221749 AS itemid, 'phenylephrine'   AS label),
    STRUCT(221986 AS itemid, 'vasopressin'     AS label),
    STRUCT(221662 AS itemid, 'dopamine'        AS label),
    STRUCT(221653 AS itemid, 'dobutamine'      AS label)
  ])
),

-- Max rates within [intime, intime+24h); phenylephrine → NE-eq (/10)
vaso_rate_max AS (
  SELECT
    fs.subject_id, fs.stay_id, vi.label AS vaso_label,
    MAX(CAST(ie.rate AS FLOAT64)) AS max_rate
  FROM first_stay fs
  JOIN `physionet-data.mimiciv_3_1_icu.inputevents` ie
    ON ie.stay_id = fs.stay_id
   AND ie.starttime >= fs.intime AND ie.starttime < fs.intime_24h
  JOIN vasopressor_ids vi
    ON vi.itemid = ie.itemid
  WHERE ie.rate IS NOT NULL
  GROUP BY fs.subject_id, fs.stay_id, vi.label
),
vaso_pivot AS (
  SELECT
    subject_id, stay_id,
    MAX(CASE WHEN vaso_label = 'norepinephrine' THEN max_rate END)        AS norepi_rate,
    MAX(CASE WHEN vaso_label = 'epinephrine'     THEN max_rate END)        AS epi_rate,
    MAX(CASE WHEN vaso_label = 'phenylephrine'   THEN max_rate/10 END)     AS pheny_as_norepi_eq_rate,
    MAX(CASE WHEN vaso_label = 'vasopressin'     THEN max_rate END)        AS vasopressin_rate,
    MAX(CASE WHEN vaso_label = 'dopamine'        THEN max_rate END)        AS dopamine_rate,
    MAX(CASE WHEN vaso_label = 'dobutamine'      THEN max_rate END)        AS dobutamine_rate
  FROM vaso_rate_max
  GROUP BY subject_id, stay_id
),

-- Min MAP in [intime, intime+24h) (for no-vaso branch); impute 71 if missing
min_map AS (
  SELECT
    fs.subject_id, fs.stay_id,
    MIN(CAST(c.valuenum AS FLOAT64)) AS min_map
  FROM first_stay fs
  JOIN `physionet-data.mimiciv_3_1_icu.chartevents` c
    ON c.stay_id = fs.stay_id
   AND c.charttime >= fs.intime AND c.charttime < fs.intime_24h
   AND c.valuenum IS NOT NULL
   AND c.itemid IN (220045, 223761) -- MAP invasive/noninvasive
  GROUP BY fs.subject_id, fs.stay_id
),

-- Cardiovascular subscore (custom rules; aligned with aSOFA/SOFA-CV)
cv_scored AS (
  SELECT
    fs.subject_id, fs.stay_id,
    COALESCE(v.norepi_rate, 0)              AS norepi_rate,
    COALESCE(v.epi_rate, 0)                 AS epi_rate,
    COALESCE(v.pheny_as_norepi_eq_rate, 0)  AS pheny_as_norepi_eq_rate,
    COALESCE(v.vasopressin_rate, 0)         AS vasopressin_rate,
    COALESCE(v.dopamine_rate, 0)            AS dopamine_rate,
    COALESCE(v.dobutamine_rate, 0)          AS dobutamine_rate,
    CASE WHEN m.min_map IS NULL THEN 71 ELSE m.min_map END AS min_map_imputed,
    CASE
      -- No vasopressors → MAP rule
      WHEN COALESCE(v.norepi_rate,0)=0 AND COALESCE(v.epi_rate,0)=0
        AND COALESCE(v.pheny_as_norepi_eq_rate,0)=0
        AND COALESCE(v.vasopressin_rate,0)=0
        AND COALESCE(v.dopamine_rate,0)=0
        AND COALESCE(v.dobutamine_rate,0)=0
        THEN CASE WHEN (CASE WHEN m.min_map IS NULL THEN 71 ELSE m.min_map END) < 70
                  THEN 1 ELSE 0 END

      -- Dobutamine any → 2
      WHEN COALESCE(v.dobutamine_rate,0) > 0 THEN 2

      -- Dopamine thresholds
      WHEN COALESCE(v.dopamine_rate,0) > 15 THEN 4
      WHEN COALESCE(v.dopamine_rate,0) > 5  THEN 3
      WHEN COALESCE(v.dopamine_rate,0) > 0  THEN 2

      -- NE/Epi/Pheny(NE-eq)
      WHEN (COALESCE(v.norepi_rate,0) > 0.1
         OR COALESCE(v.epi_rate,0) > 0.1
         OR COALESCE(v.pheny_as_norepi_eq_rate,0) > 0.1) THEN 4
      WHEN (COALESCE(v.norepi_rate,0) > 0
         OR COALESCE(v.epi_rate,0) > 0
         OR COALESCE(v.pheny_as_norepi_eq_rate,0) > 0
         OR COALESCE(v.vasopressin_rate,0) > 0) THEN 3

      ELSE 0
    END AS cardiovascular
  FROM first_stay fs
  LEFT JOIN vaso_pivot v
    ON v.subject_id = fs.subject_id AND v.stay_id = fs.stay_id
  LEFT JOIN min_map m
    ON m.subject_id = fs.subject_id AND m.stay_id = fs.stay_id
),

-- Rebuild SOFA total for Day-1 using custom CV + first_day_sofa others
sofa_rebuilt AS (
  SELECT
    d.subject_id, d.stay_id,
    -- define a "sofa_time" for pairing: end of Day-1 window
    fs.intime_24h AS sofa_time,
    d.respiration, d.coagulation, d.liver, cv.cardiovascular, d.cns, d.renal,
    (COALESCE(d.respiration,0)
     + COALESCE(d.coagulation,0)
     + COALESCE(d.liver,0)
     + COALESCE(cv.cardiovascular,0)
     + COALESCE(d.cns,0)
     + COALESCE(d.renal,0)) AS sofa_score
  FROM day1_sofa d
  JOIN first_stay fs
    ON fs.subject_id = d.subject_id AND fs.stay_id = d.stay_id
  LEFT JOIN cv_scored cv
    ON cv.subject_id = d.subject_id AND cv.stay_id = d.stay_id
),

-- Pair with suspected infection within [-48h, +24h] of Day-1 (end)
paired AS (
  SELECT
    soi.subject_id,
    soi.stay_id,
    soi.hadm_id,
    soi.ab_id,
    soi.antibiotic,
    soi.antibiotic_time,
    soi.culture_time,
    soi.suspected_infection,
    soi.suspected_infection_time,
    sr.sofa_time,
    sr.sofa_score,
    sr.respiration, sr.coagulation, sr.liver, sr.cardiovascular, sr.cns, sr.renal,
    (soi.suspected_infection = 1 AND sr.sofa_score >= 2) AS sepsis3,
    ROW_NUMBER() OVER (
      PARTITION BY soi.stay_id
      ORDER BY soi.suspected_infection_time, soi.antibiotic_time, soi.culture_time, sr.sofa_time
    ) AS rn_sus
  FROM `<PROJECT>.<DATASET>`.suspicion_of_infection AS soi
  JOIN sofa_rebuilt sr
    ON sr.stay_id = soi.stay_id
   AND sr.sofa_time >= DATETIME_SUB(soi.suspected_infection_time, INTERVAL 48 HOUR)
   AND sr.sofa_time <= DATETIME_ADD(soi.suspected_infection_time, INTERVAL 24 HOUR)
  WHERE soi.stay_id IS NOT NULL
)

-- Final output (add septic_on_admission / sepsis_within_24h flags)
SELECT
  p.subject_id, p.stay_id, p.hadm_id, p.ab_id, p.antibiotic,
  p.suspected_infection_time, p.antibiotic_time, p.culture_time,
  p.sofa_time,
  p.sofa_score, p.respiration, p.coagulation, p.liver, p.cardiovascular, p.cns, p.renal,
  p.sepsis3,
  -- Consistency flags with eICU
  CASE
    WHEN p.sepsis3 = TRUE AND p.suspected_infection_time IS NOT NULL
         AND p.suspected_infection_time <= fs.intime
    THEN TRUE ELSE FALSE
  END AS septic_on_admission,
  CASE
    WHEN p.sepsis3 = TRUE AND p.suspected_infection_time IS NOT NULL
         AND p.suspected_infection_time > fs.intime
         AND p.suspected_infection_time <= DATETIME_ADD(fs.intime, INTERVAL 24 HOUR)
    THEN TRUE ELSE FALSE
  END AS sepsis_within_24h
FROM paired p
JOIN first_stay fs
  ON fs.subject_id = p.subject_id AND fs.stay_id = p.stay_id
WHERE p.rn_sus = 1
;
