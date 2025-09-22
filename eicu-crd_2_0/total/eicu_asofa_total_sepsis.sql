-- =====================================================================
-- File: eicu_asofa_total_sepsis.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.eicu_asofa_total_sepsis
--
-- Purpose
--   Build the aSOFA vs SOFA total table for the *Sepsis-3 cohort* restricted to:
--     (1) septic on ICU admission, OR
--     (2) septic within the first 24 hours after ICU admission.
--   The cohort and timing flags come from the finalized Sepsis-3 build.
--
-- Alignment & Dependencies
--   * Sepsis-3 window/flags: <PROJECT>.<DATASET>.sepsis3
--       - adult (>=18y), FIRST ICU stay, 0..1440 min window
--       - septic_on_arrival / sepsis_within_24h available for filtering
--       - SOFA components aligned with aSOFA modules; CNS included for Sepsis-3 only
--         (see public eicu_sepsis3.sql).   [ref]  
--   * aSOFA/SOFA totals (non-CNS): follow the same structure and naming as
--     eicu_asofa_total.sql, including LOS & 30-day mortality definitions. [ref]
--     - LOS & outcomes use icustay_detail as single source of truth
--     - Coagulation from platelet_min; Liver from bilirubin (apacheapsvar)
--       (see public eicu_asofa_total.sql). 
--
-- Inputs (finalized domain tables; adult + first ICU stay logic consistent)
--   <PROJECT>.<DATASET>.eicu_asofa_cv
--   <PROJECT>.<DATASET>.eicu_asofa_resp
--   <PROJECT>.<DATASET>.eicu_asofa_renal
--   <PROJECT>.<DATASET>.sepsis3
--   physionet-data.eicu_crd_derived.icustay_detail
--   physionet-data.eicu_crd_derived.labsfirstday
--   physionet-data.eicu_crd.apacheapsvar
--
-- Output columns (matched to eicu_asofa_total for consistency)
--   patientunitstayid | age | gender | length_of_stay | mortality_30day
--   asofa_cv_score | sofa_cv_score
--   asofa_resp_score | sofa_resp_score
--   asofa_renal_score | sofa_renal_score
--   sofa_coag_score | sofa_liver_score
--   asofa_total_score | sofa_total_score
--
-- Notes
--   * Cohort: union of septic_on_arrival OR sepsis_within_24h from Sepsis-3.
--   * LOS is recomputed from icustay_detail (days), mortality from 30-day rule,
--     exactly as in eicu_asofa_total.  
--   * This script is GitHub-ready and aligned with the published eICU scripts.
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_asofa_total_sepsis AS
WITH
-- -------------------------
-- 0) Sepsis-3 cohort: septic on arrival OR within 24h
-- -------------------------
sepsis24 AS (
  SELECT DISTINCT patientunitstayid
  FROM `<PROJECT>.<DATASET>`.sepsis3
  WHERE COALESCE(septic_on_arrival, FALSE) OR COALESCE(sepsis_within_24h, FALSE)
),

-- -------------------------
-- 1) Base demographics, LOS, outcomes (same as eicu_asofa_total)
-- -------------------------
base AS (
  SELECT
    icu.patientunitstayid,
    CASE WHEN icu.age = '> 89' THEN 90 ELSE SAFE_CAST(icu.age AS INT64) END AS age,
    icu.gender,
    -- 30-day in-hospital mortality (consistent rule)
    CASE
      WHEN icu.hospitaldischargeoffset BETWEEN 0 AND 43200 AND icu.hosp_mort = 1 THEN 1
      ELSE 0
    END AS mortality_30day,
    ROUND(SAFE_CAST(icu.icu_los_hours AS FLOAT64)/24.0, 2) AS length_of_stay
  FROM `physionet-data.eicu_crd_derived.icustay_detail` icu
  WHERE icu.unitvisitnumber = 1
    AND icu.age IS NOT NULL
    AND icu.gender IS NOT NULL
    AND (icu.age = '> 89' OR SAFE_CAST(icu.age AS INT64) >= 18)
    AND icu.patientunitstayid IN (SELECT patientunitstayid FROM sepsis24)
),

-- -------------------------
-- 2) SOFA coagulation & liver (identical scoring to eicu_asofa_total)
-- -------------------------
coag_liver AS (
  SELECT
    b.patientunitstayid,
    -- SOFA coagulation (platelet)
    CASE
      WHEN l.PLATELET_min IS NULL OR l.PLATELET_min >= 150 THEN 0
      WHEN l.PLATELET_min >= 100 THEN 1
      WHEN l.PLATELET_min >= 50  THEN 2
      WHEN l.PLATELET_min >= 20  THEN 3
      ELSE 4
    END AS sofa_coag_score,
    -- SOFA liver (bilirubin, mg/dL)
    CASE
      WHEN a.bilirubin IS NULL OR a.bilirubin = -1 OR a.bilirubin < 1.2 THEN 0
      WHEN a.bilirubin < 2.0  THEN 1
      WHEN a.bilirubin < 6.0  THEN 2
      WHEN a.bilirubin < 12.0 THEN 3
      ELSE 4
    END AS sofa_liver_score
  FROM base b
  LEFT JOIN `physionet-data.eicu_crd_derived.labsfirstday` l USING (patientunitstayid)
  LEFT JOIN `physionet-data.eicu_crd.apacheapsvar` a        USING (patientunitstayid)
),

-- -------------------------
-- 3) Domain inputs (CV, Resp, Renal) — finalized modules
-- -------------------------
cv AS (
  SELECT patientunitstayid, asofa_cv_score,  sofa_cv_score
  FROM `<PROJECT>.<DATASET>`.eicu_asofa_cv
  WHERE patientunitstayid IN (SELECT patientunitstayid FROM base)
),
resp AS (
  SELECT patientunitstayid, asofa_resp_score, sofa_resp_score
  FROM `<PROJECT>.<DATASET>`.eicu_asofa_resp
  WHERE patientunitstayid IN (SELECT patientunitstayid FROM base)
),
renal AS (
  SELECT patientunitstayid, asofa_renal_score, sofa_renal_score
  FROM `<PROJECT>.<DATASET>`.eicu_asofa_renal
  WHERE patientunitstayid IN (SELECT patientunitstayid FROM base)
),

-- -------------------------
-- 4) Merge + totals
-- -------------------------
merged AS (
  SELECT
    b.patientunitstayid,
    b.age,
    b.gender,
    b.length_of_stay,
    b.mortality_30day,
    cv.asofa_cv_score,    cv.sofa_cv_score,
    rs.asofa_resp_score,  rs.sofa_resp_score,
    rn.asofa_renal_score, rn.sofa_renal_score,
    cl.sofa_coag_score,   cl.sofa_liver_score
  FROM base b
  LEFT JOIN cv  USING (patientunitstayid)
  LEFT JOIN resp rs USING (patientunitstayid)
  LEFT JOIN renal rn USING (patientunitstayid)
  LEFT JOIN coag_liver cl USING (patientunitstayid)
)

SELECT
  m.patientunitstayid,
  m.age,
  m.gender,
  m.length_of_stay,
  m.mortality_30day,

  -- Domain subscores
  m.asofa_cv_score,    m.sofa_cv_score,
  m.asofa_resp_score,  m.sofa_resp_score,
  m.asofa_renal_score, m.sofa_renal_score,

  -- SOFA coag/liver (used in both totals by design)
  COALESCE(m.sofa_coag_score, 0)  AS sofa_coag_score,
  COALESCE(m.sofa_liver_score, 0) AS sofa_liver_score,

  -- Totals (aSOFA uses SOFA coag/liver)
  ( COALESCE(m.asofa_cv_score, 0)
  + COALESCE(m.asofa_resp_score, 0)
  + COALESCE(m.asofa_renal_score, 0)
  + COALESCE(m.sofa_coag_score, 0)
  + COALESCE(m.sofa_liver_score, 0) ) AS asofa_total_score,

  ( COALESCE(m.sofa_cv_score, 0)
  + COALESCE(m.sofa_resp_score, 0)
  + COALESCE(m.sofa_renal_score, 0)
  + COALESCE(m.sofa_coag_score, 0)
  + COALESCE(m.sofa_liver_score, 0) ) AS sofa_total_score

FROM merged m
ORDER BY m.patientunitstayid;
