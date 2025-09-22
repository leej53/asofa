-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.mimic_asofa_renal
--
-- Purpose
--   Compute aSOFA (renal) and SOFA (renal) for ADULTS (age >= 18) on the FIRST ICU stay.
--   aSOFA-renal = 4 if RRT present on day 1; otherwise equals SOFA day-1 renal subscore.
--
-- Inputs
--   physionet-data.mimiciv_3_1_icu.icustays
--   physionet-data.mimiciv_3_1_hosp.patients
--   physionet-data.mimiciv_3_1_derived.first_day_rrt
--   physionet-data.mimiciv_3_1_derived.first_day_sofa
--   physionet-data.mimiciv_3_1_hosp.admissions
--
-- Outputs
--   subject_id | stay_id | asofa_renal_score | sofa_renal_score | length_of_stay | mortality_30day
--
-- Notes
--   * Adult filter (anchor_age >= 18).
--   * First ICU stay per subject.
--   * ICU LOS from icustays; 30-day in-hospital mortality from admissions.
--   * Consistent column names with other domain scripts.

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.mimic_asofa_renal AS
WITH
-- Adults (>=18) & FIRST ICU stay per subject
first_icu AS (
  SELECT
    i.subject_id,
    i.stay_id,
    i.hadm_id,
    i.intime,
    i.outtime,
    ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime) AS rn
  FROM `physionet-data.mimiciv_3_1_icu.icustays` i
  JOIN `physionet-data.mimiciv_3_1_hosp.patients` p
    ON p.subject_id = i.subject_id
  WHERE p.anchor_age >= 18
),
first_stay AS (
  SELECT subject_id, stay_id, hadm_id, intime, outtime
  FROM first_icu
  WHERE rn = 1
),

-- ICU LOS (days, 2 decimals)
icu_los AS (
  SELECT
    subject_id,
    stay_id,
    ROUND(DATETIME_DIFF(outtime, intime, HOUR)/24.0, 2) AS length_of_stay
  FROM first_stay
),

-- Day-1 RRT flag and SOFA renal
renal_sources AS (
  SELECT
    fs.subject_id,
    fs.stay_id,
    fs.hadm_id,
    COALESCE(rrt.dialysis_present, 0) AS dialysis_present,
    rrt.dialysis_type,
    COALESCE(sofa.renal, 0)           AS sofa_renal_score
  FROM first_stay fs
  LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_rrt` rrt
    ON rrt.stay_id = fs.stay_id
  LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_sofa` sofa
    ON sofa.stay_id = fs.stay_id
),

-- aSOFA-renal scoring
scored AS (
  SELECT
    subject_id,
    stay_id,
    hadm_id,
    CASE WHEN dialysis_present = 1 THEN 4 ELSE sofa_renal_score END AS asofa_renal_score,
    sofa_renal_score
  FROM renal_sources
),

-- 30-day in-hospital mortality
mort_30d AS (
  SELECT
    a.subject_id,
    a.hadm_id,
    CASE
      WHEN a.deathtime IS NOT NULL
       AND a.deathtime <= TIMESTAMP_ADD(a.admittime, INTERVAL 30 DAY)
       AND a.deathtime <= a.dischtime
      THEN 1 ELSE 0 END AS mortality_30day
  FROM `physionet-data.mimiciv_3_1_hosp.admissions` a
)

-- Final output (consistent headers)
SELECT
  s.subject_id,
  s.stay_id,
  COALESCE(s.asofa_renal_score, 0) AS asofa_renal_score,
  COALESCE(s.sofa_renal_score, 0)  AS sofa_renal_score,
  COALESCE(l.length_of_stay, 0)    AS length_of_stay,
  COALESCE(m.mortality_30day, 0)   AS mortality_30day
FROM scored s
LEFT JOIN icu_los l
  ON l.subject_id = s.subject_id AND l.stay_id = s.stay_id
LEFT JOIN mort_30d m
  ON m.subject_id = s.subject_id AND m.hadm_id = s.hadm_id
ORDER BY s.subject_id, s.stay_id;
