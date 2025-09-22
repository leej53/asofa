-- BigQuery Standard SQL
-- Project:   <YOUR_PROJECT>
-- Dataset:   <YOUR_DATASET>
-- Table out: <YOUR_PROJECT>.<YOUR_DATASET>.mimic_asofa_cv
--
-- Purpose:
--   Build cardiovascular aSOFA and SOFA scores for the FIRST ICU stay
--   during the first 24 hours, with adult filter (age >= 18),
--   conservative MAP imputation (71 mmHg when MAP missing and no vasopressors),
--   and ICU LOS + 30-day in-hospital mortality.
--
-- Key differences vs legacy:
--   * MAP imputation aligned to eICU logic to avoid spurious hypotension points.
--   * Phenylephrine handled as norepinephrine-equivalent by dividing by 10 (SOFA rule).
--   * Midodrine-only cases receive aSOFA-CV = 1 (down-weighted).
--   * Window is [intime, intime+24h).
--
-- Refs: Source pipelines consolidated & clarified (see manuscript methods).
--       Phenylephrine → NE-equivalent (/10) as in original MIMIC CV script.
--       Adult + first ICU stay filter applied at the earliest cohort CTE.

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.mimic_asofa_cv AS
WITH
-- -------------------------------------------------------------------
-- 0) Vasopressor item IDs used in MIMIC-IV v3.1 inputevents
--    (norepinephrine, epinephrine, phenylephrine, vasopressin, dopamine, dobutamine)
-- -------------------------------------------------------------------
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

-- -------------------------------------------------------------------
-- 1) Adult patients (age >= 18) & FIRST ICU stay per subject
-- -------------------------------------------------------------------
first_icu AS (
  SELECT
    i.subject_id, i.stay_id, i.hadm_id, i.intime, i.outtime,
    ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime) AS rn
  FROM `physionet-data.mimiciv_3_1_icu.icustays` i
  JOIN `physionet-data.mimiciv_3_1_hosp.patients` p
    ON p.subject_id = i.subject_id
  WHERE p.anchor_age >= 18  -- adult filter
),
first_stay AS (
  SELECT subject_id, stay_id, hadm_id, intime, outtime
  FROM first_icu
  WHERE rn = 1
),
win24h AS (
  SELECT
    subject_id, stay_id, hadm_id, intime,
    DATETIME_ADD(intime, INTERVAL 24 HOUR) AS intime_24h,
    outtime
  FROM first_stay
),

-- -------------------------------------------------------------------
-- 2) ICU LOS for the first ICU stay (in days, rounded to 2 decimals)
-- -------------------------------------------------------------------
icu_los AS (
  SELECT
    subject_id, stay_id,
    ROUND(DATETIME_DIFF(outtime, intime, HOUR)/24.0, 2) AS length_of_stay
  FROM first_stay
),

-- -------------------------------------------------------------------
-- 3) Midodrine within first 24h (used only to down-weight to aSOFA=1 when no vasopressors)
-- -------------------------------------------------------------------
midodrine_24h AS (
  SELECT DISTINCT w.subject_id, w.stay_id
  FROM win24h w
  JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` pr
    ON pr.subject_id = w.subject_id AND pr.hadm_id = w.hadm_id
  WHERE LOWER(pr.drug) LIKE '%midodrine%'
    AND pr.starttime >= w.intime AND pr.starttime < w.intime_24h
),

-- -------------------------------------------------------------------
-- 4) Distinct vasopressors used within first 24h (for aSOFA-CV count)
-- -------------------------------------------------------------------
vaso_distinct_24h AS (
  SELECT
    w.subject_id, w.stay_id,
    COUNT(DISTINCT vi.itemid) AS n_vaso
  FROM win24h w
  JOIN `physionet-data.mimiciv_3_1_icu.inputevents` ie
    ON ie.stay_id = w.stay_id
   AND ie.starttime >= w.intime AND ie.starttime < w.intime_24h
  JOIN vasopressor_ids vi
    ON vi.itemid = ie.itemid
  GROUP BY w.subject_id, w.stay_id
),

-- -------------------------------------------------------------------
-- 5) aSOFA-CV score based on number of distinct vasopressors (midodrine-only=1)
-- -------------------------------------------------------------------
asofa_cv AS (
  SELECT
    w.subject_id, w.stay_id, w.hadm_id,
    CASE
      WHEN vd.n_vaso IS NULL AND m.subject_id IS NOT NULL THEN 1    -- midodrine only
      WHEN vd.n_vaso = 1 THEN 2
      WHEN vd.n_vaso = 2 THEN 3
      WHEN vd.n_vaso >= 3 THEN 4
      ELSE 0
    END AS asofa_cv_score
  FROM win24h w
  LEFT JOIN vaso_distinct_24h vd
    ON vd.subject_id = w.subject_id AND vd.stay_id = w.stay_id
  LEFT JOIN midodrine_24h m
    ON m.subject_id = w.subject_id AND m.stay_id = w.stay_id
),

-- -------------------------------------------------------------------
-- 6) Max infusion rates by agent within first 24h (for SOFA-CV dose thresholds)
--    Phenylephrine is converted to NE-equivalent by dividing by 10.
-- -------------------------------------------------------------------
vaso_rate_max AS (
  SELECT
    w.subject_id, w.stay_id, vi.label AS vaso_label,
    MAX(CAST(ie.rate AS FLOAT64)) AS max_rate
  FROM win24h w
  JOIN `physionet-data.mimiciv_3_1_icu.inputevents` ie
    ON ie.stay_id = w.stay_id
   AND ie.starttime >= w.intime AND ie.starttime < w.intime_24h
  JOIN vasopressor_ids vi
    ON vi.itemid = ie.itemid
  WHERE ie.rate IS NOT NULL
  GROUP BY w.subject_id, w.stay_id, vi.label
),
vaso_pivot AS (
  SELECT
    subject_id, stay_id,
    MAX(CASE WHEN vaso_label = 'norepinephrine' THEN max_rate END)         AS norepi_rate,
    MAX(CASE WHEN vaso_label = 'epinephrine'     THEN max_rate END)         AS epi_rate,
    -- phenylephrine → norepinephrine-equivalent (/10)
    MAX(CASE WHEN vaso_label = 'phenylephrine'   THEN max_rate/10 END)      AS pheny_as_norepi_eq_rate,
    MAX(CASE WHEN vaso_label = 'vasopressin'     THEN max_rate END)         AS vasopressin_rate,
    MAX(CASE WHEN vaso_label = 'dopamine'        THEN max_rate END)         AS dopamine_rate,
    MAX(CASE WHEN vaso_label = 'dobutamine'      THEN max_rate END)         AS dobutamine_rate
  FROM vaso_rate_max
  GROUP BY subject_id, stay_id
),

-- -------------------------------------------------------------------
-- 7) Min MAP within first 24h (from chartevents), used only if no vasopressors
-- -------------------------------------------------------------------
min_map AS (
  SELECT
    w.subject_id, w.stay_id,
    MIN(CAST(c.valuenum AS FLOAT64)) AS min_map
  FROM win24h w
  JOIN `physionet-data.mimiciv_3_1_icu.chartevents` c
    ON c.stay_id = w.stay_id
   AND c.charttime >= w.intime AND c.charttime < w.intime_24h
   AND c.valuenum IS NOT NULL
   AND c.itemid IN (220045, 223761) -- MAP invasive/noninvasive
  GROUP BY w.subject_id, w.stay_id
),

-- -------------------------------------------------------------------
-- 8) SOFA-CV calculation
--    * If NO vasopressors: hypotension point assigned when min MAP < 70.
--      When MAP is missing, we conservatively impute 71 mmHg (0 points).
--    * If vasopressors present: standard dose thresholds (NE/Epi >0.1 → 4; etc.).
-- -------------------------------------------------------------------
sofa_cv AS (
  SELECT
    v.subject_id, v.stay_id,
    COALESCE(v.norepi_rate, 0)              AS norepi_rate,
    COALESCE(v.epi_rate, 0)                 AS epi_rate,
    COALESCE(v.pheny_as_norepi_eq_rate, 0)  AS pheny_as_norepi_eq_rate,
    COALESCE(v.vasopressin_rate, 0)         AS vasopressin_rate,
    COALESCE(v.dopamine_rate, 0)            AS dopamine_rate,
    COALESCE(v.dobutamine_rate, 0)          AS dobutamine_rate,
    -- conservative imputation for missing MAP (when referenced under no-vaso branch)
    CASE WHEN m.min_map IS NULL THEN 71 ELSE m.min_map END AS min_map_imputed,
    CASE
      -- No vasopressors: use imputed MAP for hypotension point (min_map < 70 → 1)
      WHEN COALESCE(v.norepi_rate,0)=0 AND COALESCE(v.epi_rate,0)=0
        AND COALESCE(v.pheny_as_norepi_eq_rate,0)=0
        AND COALESCE(v.vasopressin_rate,0)=0
        AND COALESCE(v.dopamine_rate,0)=0
        AND COALESCE(v.dobutamine_rate,0)=0
        THEN CASE WHEN (CASE WHEN m.min_map IS NULL THEN 71 ELSE m.min_map END) < 70
                  THEN 1 ELSE 0 END

      -- Dobutamine any dose → 2
      WHEN COALESCE(v.dobutamine_rate,0) > 0 THEN 2

      -- Dopamine 5–15 → 3; >15 → 4; ≤5 → 2
      WHEN COALESCE(v.dopamine_rate,0) > 15 THEN 4
      WHEN COALESCE(v.dopamine_rate,0) > 5  THEN 3
      WHEN COALESCE(v.dopamine_rate,0) > 0  THEN 2

      -- NE/Epi/Pheny(NE-eq) > 0.1 → 4; (0, 0.1] → 3
      WHEN (COALESCE(v.norepi_rate,0) > 0.1
         OR COALESCE(v.epi_rate,0) > 0.1
         OR COALESCE(v.pheny_as_norepi_eq_rate,0) > 0.1) THEN 4
      WHEN (COALESCE(v.norepi_rate,0) > 0
         OR COALESCE(v.epi_rate,0) > 0
         OR COALESCE(v.pheny_as_norepi_eq_rate,0) > 0
         OR COALESCE(v.vasopressin_rate,0) > 0) THEN 3

      ELSE 0
    END AS sofa_cv_score
  FROM vaso_pivot v
  LEFT JOIN min_map m
    ON m.subject_id = v.subject_id AND m.stay_id = v.stay_id
),

-- -------------------------------------------------------------------
-- 9) 30-day in-hospital mortality (admissions-based)
-- -------------------------------------------------------------------
mort_30d AS (
  SELECT
    a.subject_id, a.hadm_id,
    CASE
      WHEN a.deathtime IS NOT NULL
       AND a.deathtime <= TIMESTAMP_ADD(a.admittime, INTERVAL 30 DAY)
       AND a.deathtime <= a.dischtime
      THEN 1 ELSE 0 END AS mortality_30day
  FROM `physionet-data.mimiciv_3_1_hosp.admissions` a
)

-- -------------------------------------------------------------------
-- 10) Final output
-- -------------------------------------------------------------------
SELECT
  a.subject_id,
  a.stay_id,
  a.asofa_cv_score,
  s.sofa_cv_score,
  COALESCE(l.length_of_stay, 0) AS length_of_stay,
  COALESCE(m.mortality_30day, 0) AS mortality_30day
FROM asofa_cv a
LEFT JOIN sofa_cv s
  ON s.subject_id = a.subject_id AND s.stay_id = a.stay_id
LEFT JOIN icu_los l
  ON l.subject_id = a.subject_id AND l.stay_id = a.stay_id
LEFT JOIN mort_30d m
  ON m.subject_id = a.subject_id
LEFT JOIN first_stay fs
  ON fs.subject_id = a.subject_id AND fs.stay_id = a.stay_id
WHERE TRUE
-- (optional) ensure mortality join on the same hospitalization
AND fs.hadm_id = m.hadm_id
ORDER BY a.subject_id, a.stay_id;