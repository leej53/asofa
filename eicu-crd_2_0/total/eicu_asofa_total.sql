-- =====================================================================
-- File: eicu_asofa_total.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.eicu_asofa_total
--
-- Purpose
--   Merge domain‑level subscores (CV, Resp, Renal) with SOFA coagulation & liver
--   computed here to produce total aSOFA and total SOFA for the FIRST ICU stay
--   (adults). Aligns naming with MIMIC totals.
--
-- Inputs (already built domain tables with consistent adult + first‑stay logic)
--   <PROJECT>.<DATASET>.eicu_asofa_cv
--     Columns: patientunitstayid, asofa_cv_score,  sofa_cv_score,  length_of_stay, mortality_30day
--   <PROJECT>.<DATASET>.eicu_asofa_resp
--     Columns: patientunitstayid, asofa_resp_score, sofa_resp_score, length_of_stay, mortality_30day
--   <PROJECT>.<DATASET>.eicu_asofa_renal
--     Columns: patientunitstayid, asofa_renal_score, sofa_renal_score, length_of_stay, mortality_30day
--
--   physionet-data.eicu_crd_derived.icustay_detail  (age, gender, LOS hours, mortality)
--   physionet-data.eicu_crd_derived.labsfirstday    (platelet_min)
--   physionet-data.eicu_crd.apacheapsvar            (bilirubin)
--
-- Output columns
--   patientunitstayid | age | gender | length_of_stay | mortality_30day
--   asofa_cv_score | sofa_cv_score
--   asofa_resp_score | sofa_resp_score
--   asofa_renal_score | sofa_renal_score
--   sofa_coag_score | sofa_liver_score
--   asofa_total_score | sofa_total_score
--
-- Notes
--   * LOS is recomputed from icustay_detail (days) for consistency; outcomes
--     and demographics use icustay_detail as single source of truth.
--   * Coagulation = platelet_min; Liver = bilirubin (apacheapsvar). Missing
--     or sentinel values (-1) map to 0 per conventional SOFA implementation.
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_asofa_total AS
WITH base AS (
  SELECT
    icu.patientunitstayid,
    CASE WHEN icu.age = '> 89' THEN 90 ELSE SAFE_CAST(icu.age AS INT64) END AS age,
    icu.gender,
    CASE WHEN icu.hospitaldischargeoffset BETWEEN 0 AND 43200 AND icu.hosp_mort = 1 THEN 1 ELSE 0 END AS mortality_30day,
    ROUND(SAFE_CAST(icu.icu_los_hours AS FLOAT64)/24.0, 2) AS length_of_stay
  FROM `physionet-data.eicu_crd_derived.icustay_detail` icu
  WHERE icu.age IS NOT NULL
    AND icu.gender IS NOT NULL
    AND icu.unitvisitnumber = 1
    AND (icu.age = '> 89' OR SAFE_CAST(icu.age AS INT64) >= 18)
),
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
cv AS (
  SELECT patientunitstayid, asofa_cv_score,  sofa_cv_score
  FROM `<PROJECT>.<DATASET>`.eicu_asofa_cv
),
resp AS (
  SELECT patientunitstayid, asofa_resp_score, sofa_resp_score
  FROM `<PROJECT>.<DATASET>`.eicu_asofa_resp
),
renal AS (
  SELECT patientunitstayid, asofa_renal_score, sofa_renal_score
  FROM `<PROJECT>.<DATASET>`.eicu_asofa_renal
),
merged AS (
  SELECT
    b.patientunitstayid,
    b.age,
    b.gender,
    b.length_of_stay,
    b.mortality_30day,
    cv.asofa_cv_score,   cv.sofa_cv_score,
    rs.asofa_resp_score, rs.sofa_resp_score,
    rn.asofa_renal_score, rn.sofa_renal_score,
    cl.sofa_coag_score,  cl.sofa_liver_score
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
  m.asofa_cv_score,   m.sofa_cv_score,
  m.asofa_resp_score, m.sofa_resp_score,
  m.asofa_renal_score, m.sofa_renal_score,
  -- SOFA coag/liver
  COALESCE(m.sofa_coag_score, 0)  AS sofa_coag_score,
  COALESCE(m.sofa_liver_score, 0) AS sofa_liver_score,
  -- Totals (aSOFA uses SOFA coag/liver by study design)
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