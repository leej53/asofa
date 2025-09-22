-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.mimic_asofa_total
--
-- Purpose
--   Merge domain-level aSOFA/SOFA subscores (CV, Resp, Renal) to produce
--   total aSOFA and total SOFA for the FIRST ICU stay (adults only), with
--   ICU length of stay and 30-day in-hospital mortality.
--
-- Inputs (already built domain tables with consistent adult + first-stay logic)
--   <PROJECT>.<DATASET>.mimic_asofa_cv
--     Columns: subject_id, stay_id, asofa_cv_score,  sofa_cv_score,  length_of_stay, mortality_30day
--              (Back-compat supported)
--   <PROJECT>.<DATASET>.mimic_asofa_resp
--     Columns: subject_id, stay_id, asofa_resp_score, sofa_resp_score, length_of_stay, mortality_30day
--              (Back-compat supported)
--   <PROJECT>.<DATASET>.mimic_asofa_renal
--     Columns: subject_id, stay_id, asofa_renal_score, sofa_renal_score, length_of_stay, mortality_30day
--              (Back-compat supported)
--
--   physionet-data.mimiciv_3_1_derived.first_day_sofa  (for coagulation, liver)
--
-- Output columns
--   subject_id | stay_id
--   asofa_cv_score | sofa_cv_score
--   asofa_resp_score | sofa_resp_score
--   asofa_renal_score | sofa_renal_score
--   sofa_coag_score | sofa_liver_score
--   asofa_total_score | sofa_total_score
--   length_of_stay | mortality_30day
--
-- Notes
--   * If the three domain tables carry slightly different LOS/mort values, we
--     take the first non-null via COALESCE(cv → resp → renal).
--   * No semicolon after CREATE ... AS; WITH must follow immediately.

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.mimic_asofa_total AS
WITH
cv AS (
  SELECT
    subject_id,
    stay_id,
    COALESCE(asofa_cv_score, 0) AS asofa_cv_score,
    -- Back-compat: allow either 'sofa_cv_score' 
    COALESCE(sofa_cv_score, 0) AS sofa_cv_score,
    SAFE_CAST(length_of_stay AS FLOAT64) AS cv_los,
    SAFE_CAST(mortality_30day AS INT64)  AS cv_mort
  FROM `<PROJECT>.<DATASET>`.mimic_asofa_cv
),

resp AS (
  SELECT
    subject_id,
    stay_id,
    COALESCE(asofa_resp_score, 0) AS asofa_resp_score,
    COALESCE(sofa_resp_score, 0) AS sofa_resp_score,
    SAFE_CAST(length_of_stay AS FLOAT64) AS resp_los,
    SAFE_CAST(mortality_30day AS INT64)  AS resp_mort
  FROM `<PROJECT>.<DATASET>`.mimic_asofa_resp
),

renal AS (
  SELECT
    subject_id,
    stay_id,
    COALESCE(asofa_renal_score, 0) AS asofa_renal_score,
    COALESCE(sofa_renal_score, 0) AS sofa_renal_score,
    SAFE_CAST(length_of_stay AS FLOAT64) AS renal_los,
    SAFE_CAST(mortality_30day AS INT64)  AS renal_mort
  FROM `<PROJECT>.<DATASET>`.mimic_asofa_renal
),

-- First-day SOFA coagulation & liver (unchanged from MIT-LCP)
sofa_extra AS (
  SELECT
    stay_id,
    COALESCE(coagulation, 0) AS sofa_coag_score,
    COALESCE(liver, 0)       AS sofa_liver_score
  FROM `physionet-data.mimiciv_3_1_derived.first_day_sofa`
),

-- Merge domain tables by (subject_id, stay_id)
merged AS (
  SELECT
    COALESCE(cv.subject_id, resp.subject_id, renal.subject_id) AS subject_id,
    COALESCE(cv.stay_id,    resp.stay_id,    renal.stay_id)    AS stay_id,

    COALESCE(cv.asofa_cv_score, 0)      AS asofa_cv_score,
    COALESCE(cv.sofa_cv_score, 0)       AS sofa_cv_score,

    COALESCE(resp.asofa_resp_score, 0)  AS asofa_resp_score,
    COALESCE(resp.sofa_resp_score, 0)   AS sofa_resp_score,

    COALESCE(renal.asofa_renal_score, 0) AS asofa_renal_score,
    COALESCE(renal.sofa_renal_score, 0)  AS sofa_renal_score,

    -- Prefer any available LOS/mortality from the three sources for this stay
    COALESCE(cv.cv_los, resp.resp_los, renal.renal_los)     AS length_of_stay,
    COALESCE(cv.cv_mort, resp.resp_mort, renal.renal_mort)  AS mortality_30day
  FROM cv
  FULL OUTER JOIN resp USING (subject_id, stay_id)
  FULL OUTER JOIN renal USING (subject_id, stay_id)
)

SELECT
  m.subject_id,
  m.stay_id,

  -- Domain subscores
  m.asofa_cv_score,    m.sofa_cv_score,
  m.asofa_resp_score,  m.sofa_resp_score,
  m.asofa_renal_score, m.sofa_renal_score,

  -- SOFA coagulation/liver (reported and used in totals)
  COALESCE(se.sofa_coag_score, 0)  AS sofa_coag_score,
  COALESCE(se.sofa_liver_score, 0) AS sofa_liver_score,

  -- Totals
  ( COALESCE(m.asofa_cv_score, 0)
  + COALESCE(m.asofa_resp_score, 0)
  + COALESCE(m.asofa_renal_score, 0)
  + COALESCE(se.sofa_coag_score, 0)
  + COALESCE(se.sofa_liver_score, 0) ) AS asofa_total_score,

  ( COALESCE(m.sofa_cv_score, 0)
  + COALESCE(m.sofa_resp_score, 0)
  + COALESCE(m.sofa_renal_score, 0)
  + COALESCE(se.sofa_coag_score, 0)
  + COALESCE(se.sofa_liver_score, 0) ) AS sofa_total_score,

  -- Outcomes
  m.length_of_stay,
  m.mortality_30day

FROM merged m
LEFT JOIN sofa_extra se
  ON se.stay_id = m.stay_id
ORDER BY m.subject_id, m.stay_id;