-- =====================================================================
-- File: eicu_asofa_cv.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.eicu_asofa_cv
--
-- Purpose
--   Compute aSOFA-CV and SOFA-CV for the FIRST ICU stay in eICU-CRD adults,
--   using the first 24h window. Aligns naming and headers with MIMIC outputs.
--
-- Inputs
--   <PROJECT>.<DATASET>.eicu_vasopressors_map (built above)
--
-- Output columns (aligned with MIMIC)
--   patientunitstayid | asofa_cv_score | sofa_cv_score | length_of_stay | mortality_30day
--
-- Scoring rules
--   aSOFA-CV: based on the NUMBER of distinct vasopressors in first 24h
--     * Count these agents if present (>0 or flag=1): norepi, epi, dopamine,
--       dobutamine, phenylephrine, vasopressin (presence only)
--       n>=3 → 4; n=2 → 3; n=1 → 2; none but midodrine-only → 1; else 0
--   SOFA-CV: per original thresholds (vasopressin EXCLUDED from dose rules)
--     * No vasopressors (norepi/epi/dopa/dobu/phenyl all zero) → min MAP < 70 → 1 else 0
--     * Dobutamine any → 2
--     * Dopamine 0–5 → 2; 5–15 → 3; >15 → 4
--     * NE/Epi/Phenyl(NE-equivalent=phenyl/10): (0,0.1] → 3; >0.1 → 4
--
-- Transparency for reviewers
--   * When MAP is entirely missing and no vasopressors are recorded, we
--     conservatively impute MAP=71 mmHg to avoid spurious hypotension points.
--     (Documented in manuscript Methods.)
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_asofa_cv AS
WITH src AS (
  SELECT * FROM `<PROJECT>.<DATASET>`.eicu_vasopressors_map
),
-- aSOFA-CV based on distinct agent count (including vasopressin presence)
cv_asofa AS (
  SELECT
    s.patientunitstayid,
    -- Count distinct agents present (>0 or flag=1)
    (IF(IFNULL(s.norepinephrine, 0) > 0, 1, 0)
     + IF(IFNULL(s.epinephrine, 0)   > 0, 1, 0)
     + IF(IFNULL(s.dopamine, 0)      > 0, 1, 0)
     + IF(IFNULL(s.dobutamine, 0)    > 0, 1, 0)
     + IF(IFNULL(s.phenylephrine, 0) > 0, 1, 0)
     + IF(IFNULL(s.vasopressin_flag, 0) = 1, 1, 0)) AS n_vaso,
    s.midodrine
  FROM src s
),
cv_asofa_scored AS (
  SELECT
    patientunitstayid,
    CASE
      WHEN n_vaso >= 3 THEN 4
      WHEN n_vaso = 2  THEN 3
      WHEN n_vaso = 1  THEN 2
      ELSE CASE WHEN midodrine = 1 THEN 1 ELSE 0 END
    END AS asofa_cv_score
  FROM cv_asofa
),
-- SOFA-CV per original dose thresholds (vasopressin excluded)
cv_sofa_scored AS (
  SELECT
    s.patientunitstayid,
    CASE
      -- No vaso branch (exclude vasopressin from this check by design)
      WHEN IFNULL(s.norepinephrine,0)=0
        AND IFNULL(s.epinephrine,0)=0
        AND IFNULL(s.dopamine,0)=0
        AND IFNULL(s.dobutamine,0)=0
        AND IFNULL(s.phenylephrine,0)=0
      THEN CASE WHEN s.map < 70 THEN 1 ELSE 0 END

      ELSE GREATEST(
        -- Dopamine
        CASE WHEN IFNULL(s.dopamine,0)    > 15 THEN 4
             WHEN IFNULL(s.dopamine,0)    >  5 THEN 3
             WHEN IFNULL(s.dopamine,0)    >  0 THEN 2
             ELSE 0 END,
        -- Dobutamine
        CASE WHEN IFNULL(s.dobutamine,0)  >  0 THEN 2 ELSE 0 END,
        -- Epinephrine
        CASE WHEN IFNULL(s.epinephrine,0) > 0.1 THEN 4
             WHEN IFNULL(s.epinephrine,0) > 0    THEN 3
             ELSE 0 END,
        -- Norepinephrine
        CASE WHEN IFNULL(s.norepinephrine,0) > 0.1 THEN 4
             WHEN IFNULL(s.norepinephrine,0) > 0    THEN 3
             ELSE 0 END,
        -- Phenylephrine → NE-equivalent (/10)
        CASE WHEN IFNULL(s.phenylephrine,0)/10 > 0.1 THEN 4
             WHEN IFNULL(s.phenylephrine,0)/10 > 0    THEN 3
             ELSE 0 END
      )
    END AS sofa_cv_score
  FROM src s
)
SELECT
  s.patientunitstayid,
  a.asofa_cv_score,
  c.sofa_cv_score,
  -- pass-through outcomes for alignment with MIMIC
  SAFE_CAST(s.length_of_stay AS FLOAT64) AS length_of_stay,
  SAFE_CAST(s.mortality_30day AS INT64)  AS mortality_30day
FROM src s
LEFT JOIN cv_asofa_scored a USING (patientunitstayid)
LEFT JOIN cv_sofa_scored  c USING (patientunitstayid)
ORDER BY s.patientunitstayid;
