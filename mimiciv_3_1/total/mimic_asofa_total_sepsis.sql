-- =====================================================================
-- File: mimic_asofa_total_sepsis.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.mimic_asofa_total_sepsis
--
-- Purpose
--   Build the aSOFA vs SOFA total table for the *Sepsis-3 cohort* restricted to:
--     (1) septic on ICU admission, OR
--     (2) septic within the first 24 hours after ICU admission.
--   Cohort filtering now uses the two flags precomputed in <PROJECT>.<DATASET>.sepsis3
--   for full parity with the eICU pipeline.
--
-- Alignment & Dependencies
--   * Sepsis-3: <PROJECT>.<DATASET>.sepsis3
--       - adult (>=18y), FIRST ICU stay; Day-1 SOFA rebuild (custom CV rules)
--       - includes: sepsis3, suspected_infection_time, septic_on_admission, sepsis_within_24h  [ref]
--   * Suspicion-of-infection rule identical to the finalized approach
--     (abx–culture timing windows). [ref]
--
-- Inputs (finalized domain tables; adult + first ICU stay already applied)
--   <PROJECT>.<DATASET>.mimic_asofa_cv
--   <PROJECT>.<DATASET>.mimic_asofa_resp
--   <PROJECT>.<DATASET>.mimic_asofa_renal
--   physionet-data.mimiciv_3_1_derived.first_day_sofa  (SOFA coagulation & liver)
--   <PROJECT>.<DATASET>.sepsis3
--
-- Output columns (matched to mimic_asofa_total for consistency)
--   subject_id | stay_id
--   asofa_cv_score | sofa_cv_score
--   asofa_resp_score | sofa_resp_score
--   asofa_renal_score | sofa_renal_score
--   sofa_coag_score | sofa_liver_score
--   asofa_total_score | sofa_total_score
--   length_of_stay | mortality_30day
--
-- Notes
--   * Cohort = sepsis3=TRUE AND (septic_on_admission OR sepsis_within_24h) from sepsis3 table.
--   * Coagulation/liver subscores are taken from first_day_sofa (unchanged).
--   * LOS/mortality are taken from domain tables as in your released code.
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.mimic_asofa_total_sepsis AS
WITH
-- Sepsis-3 cohort: use precomputed flags for perfect parity with eICU
sepsis24 AS (
  SELECT DISTINCT s.stay_id
  FROM `<PROJECT>.<DATASET>`.sepsis3 AS s
  WHERE s.sepsis3 = TRUE
    AND (COALESCE(s.septic_on_admission, FALSE) OR COALESCE(s.sepsis_within_24h, FALSE))
),

-- aSOFA/SOFA domain inputs (CV, Resp, Renal), filtered to the Sepsis-3 cohort
cv AS (
  SELECT
    subject_id,
    stay_id,
    COALESCE(asofa_cv_score, 0) AS asofa_cv_score,
    COALESCE(sofa_cv_score, 0)  AS sofa_cv_score,
    SAFE_CAST(length_of_stay AS FLOAT64) AS cv_los,
    SAFE_CAST(mortality_30day AS INT64)  AS cv_mort
  FROM `<PROJECT>.<DATASET>`.mimic_asofa_cv
  INNER JOIN sepsis24 USING (stay_id)
),

resp AS (
  SELECT
    subject_id,
    stay_id,
    COALESCE(asofa_resp_score, 0) AS asofa_resp_score,
    COALESCE(sofa_resp_score, 0)  AS sofa_resp_score,
    SAFE_CAST(length_of_stay AS FLOAT64) AS resp_los,
    SAFE_CAST(mortality_30day AS INT64)  AS resp_mort
  FROM `<PROJECT>.<DATASET>`.mimic_asofa_resp
  INNER JOIN sepsis24 USING (stay_id)
),

renal AS (
  SELECT
    subject_id,
    stay_id,
    COALESCE(asofa_renal_score, 0) AS asofa_renal_score,
    COALESCE(sofa_renal_score, 0)  AS sofa_renal_score,
    SAFE_CAST(length_of_stay AS FLOAT64) AS renal_los,
    SAFE_CAST(mortality_30day AS INT64)  AS renal_mort
  FROM `<PROJECT>.<DATASET>`.mimic_asofa_renal
  INNER JOIN sepsis24 USING (stay_id)
),

-- First-day SOFA coagulation & liver (unchanged; used in totals)
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

    -- LOS/mortality: follow domain-table source as per released code
    COALESCE(cv.cv_los, resp.resp_los, renal.renal_los)     AS length_of_stay,
    COALESCE(cv.cv_mort, resp.resp_mort, renal.renal_mort)  AS mortality_30day
  FROM cv
  FULL OUTER JOIN resp  USING (subject_id, stay_id)
  FULL OUTER JOIN renal USING (subject_id, stay_id)
)

SELECT
  m.subject_id,
  m.stay_id,

  -- Domain subscores
  m.asofa_cv_score,    m.sofa_cv_score,
  m.asofa_resp_score,  m.sofa_resp_score,
  m.asofa_renal_score, m.sofa_renal_score,

  -- SOFA coagulation/liver (used in both totals)
  COALESCE(se.sofa_coag_score, 0)  AS sofa_coag_score,
  COALESCE(se.sofa_liver_score, 0) AS sofa_liver_score,

  -- Totals (match mimic_asofa_total)
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
