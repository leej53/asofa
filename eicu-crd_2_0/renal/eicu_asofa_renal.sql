-- =====================================================================
-- File: eicu_asofa_renal.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.eicu_asofa_renal
--
-- Purpose
--   Compute aSOFA (renal) and SOFA (renal) for ADULTS on the FIRST ICU stay
--   in eICU‑CRD. aSOFA‑renal = 4 if dialysis present on day 1; otherwise equals
--   SOFA renal subscore from day‑1 creatinine. Output headers are aligned with
--   MIMIC code for downstream merging.
--
-- Inputs
--   physionet-data.eicu_crd_derived.icustay_detail   (age, sex, LOS hours, mortality)
--   physionet-data.eicu_crd.apacheapsvar            (day‑1 creatinine, dialysis)
--
-- Outputs (aligned with MIMIC)
--   patientunitstayid | asofa_renal_score | sofa_renal_score | length_of_stay | mortality_30day
--
-- Notes
--   * Adults only (>=18; '> 89' → 90). First ICU visit (unitvisitnumber = 1).
--   * ICU LOS converted to DAYS (hours/24), rounded to 2 decimals to match MIMIC.
--   * Creatinine: treat NULL or -1 as missing → score 0 per conservative rule.
--   * Dialysis: binary flag in apacheapsvar; if 1, aSOFA renal = 4 (override).
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_asofa_renal AS
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
renal_src AS (
  SELECT
    b.patientunitstayid,
    b.length_of_stay,
    b.mortality_30day,
    a.creatinine,
    a.dialysis
  FROM base b
  LEFT JOIN `physionet-data.eicu_crd.apacheapsvar` a
    USING (patientunitstayid)
),
scored AS (
  SELECT
    patientunitstayid,
    -- SOFA renal (creatinine, mg/dL)
    CASE
      WHEN creatinine IS NULL OR creatinine = -1 OR creatinine < 1.2 THEN 0
      WHEN creatinine < 2.0  THEN 1
      WHEN creatinine < 3.5  THEN 2
      WHEN creatinine < 5.0  THEN 3
      ELSE 4
    END AS sofa_renal_score,
    -- aSOFA renal override by dialysis
    CASE
      WHEN dialysis = 1 THEN 4
      ELSE CASE
        WHEN creatinine IS NULL OR creatinine = -1 OR creatinine < 1.2 THEN 0
        WHEN creatinine < 2.0  THEN 1
        WHEN creatinine < 3.5  THEN 2
        WHEN creatinine < 5.0  THEN 3
        ELSE 4
      END
    END AS asofa_renal_score
  FROM renal_src
)
SELECT
  s.patientunitstayid,
  s.asofa_renal_score,
  s.sofa_renal_score,
  r.length_of_stay,
  r.mortality_30day
FROM scored s
JOIN renal_src r USING (patientunitstayid)
ORDER BY s.patientunitstayid;