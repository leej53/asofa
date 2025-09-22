-- =====================================================================
-- File: eicu_vasopressors_map.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.eicu_vasopressors_map
--
-- Purpose
--   Build a FIRST-24H cardiovascular mapping table for the FIRST ICU stay
--   in eICU-CRD adults, including:
--     * Max infusion rates (mcg/kg/min) for vaso agents used in SOFA rules
--       (norepinephrine, epinephrine, dopamine, dobutamine, phenylephrine)
--     * Vasopressin PRESENCE ONLY (binary), for aSOFA-CV counting (not used
--       in SOFA dose thresholds per original SOFA definition)
--     * min MAP (IBP preferred, else NIBP) with conservative imputation
--       deferred to downstream consumer
--     * Midodrine flag within first 24h
--     * ICU length_of_stay (days) and 30-day in-hospital mortality
--
-- Cohort
--   Adults (>=18, with '> 89' coded as 90), FIRST ICU visit (unitvisitnumber=1).
--   Window for all day-1 features: [0, 1440) minutes from unit admission.
--
-- Notes
--   * Rates normalized to mcg/kg/min when drugrate is not in mcg/kg/min
--     by dividing by admission weight (kg). If weight missing or zero,
--     division safely yields NULL, which is ignored by MAX.
--   * Phenylephrine NE-equivalent conversion (/10) is applied downstream
--     when computing SOFA (not here), to keep this map strictly observational.
--   * Vasopressin is captured only as presence (binary) to support aSOFA counts.
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_vasopressors_map AS
WITH mortality AS (
  SELECT
    icu.patientunitstayid,
    CASE WHEN icu.age = '> 89' THEN 90 ELSE SAFE_CAST(icu.age AS INT64) END AS age,
    icu.gender,
    -- ICU LOS in days (convert from hours to days, 2 decimals for consistency)
    ROUND(SAFE_CAST(icu.icu_los_hours AS FLOAT64) / 24.0, 2) AS length_of_stay,
    -- 30-day in-hospital mortality: discharge within 30 days and died in hospital
    CASE
      WHEN icu.hospitaldischargeoffset BETWEEN 0 AND 43200 AND icu.hosp_mort = 1 THEN 1
      ELSE 0
    END AS mortality_30day
  FROM `physionet-data.eicu_crd_derived.icustay_detail` AS icu
  WHERE icu.age IS NOT NULL
    AND icu.gender IS NOT NULL
    AND icu.unitvisitnumber = 1
    AND ((icu.age != '> 89' AND SAFE_CAST(icu.age AS INT64) >= 18) OR icu.age = '> 89')
),
-- Vaso agents used in SOFA thresholds (rates kept); vasopressin handled separately
vaso_raw AS (
  SELECT
    i.patientunitstayid,
    CASE
      WHEN LOWER(i.drugname) LIKE '%norepinephrine%' THEN 'norepinephrine'
      WHEN LOWER(i.drugname) LIKE '%epinephrine%'     THEN 'epinephrine'
      WHEN LOWER(i.drugname) LIKE '%dopamine%'        THEN 'dopamine'
      WHEN LOWER(i.drugname) LIKE '%dobutamine%'      THEN 'dobutamine'
      WHEN LOWER(i.drugname) LIKE '%phenylephrine%'   THEN 'phenylephrine'
      ELSE NULL
    END AS drug,
    CASE
      WHEN LOWER(i.drugname) LIKE '%mcg/kg/min%' THEN SAFE_CAST(i.drugrate AS FLOAT64)
      ELSE SAFE_CAST(i.drugrate AS FLOAT64) / NULLIF(p.admissionweight, 0)
    END AS drugrate_adj
  FROM `physionet-data.eicu_crd.infusiondrug` AS i
  LEFT JOIN `physionet-data.eicu_crd.patient` AS p
    ON p.patientunitstayid = i.patientunitstayid
  WHERE i.infusionoffset BETWEEN 0 AND 1440
    AND (i.drugrate IS NOT NULL AND SAFE_CAST(i.drugrate AS FLOAT64) != 0)
    AND (
      LOWER(i.drugname) LIKE '%norepinephrine%'
      OR LOWER(i.drugname) LIKE '%epinephrine%'
      OR LOWER(i.drugname) LIKE '%dopamine%'
      OR LOWER(i.drugname) LIKE '%dobutamine%'
      OR LOWER(i.drugname) LIKE '%phenylephrine%'
    )
),
vaso AS (
  SELECT
    patientunitstayid,
    ROUND(MAX(IF(drug = 'norepinephrine', drugrate_adj, NULL)), 2) AS norepinephrine,
    ROUND(MAX(IF(drug = 'epinephrine',     drugrate_adj, NULL)), 2) AS epinephrine,
    ROUND(MAX(IF(drug = 'dopamine',        drugrate_adj, NULL)), 2) AS dopamine,
    ROUND(MAX(IF(drug = 'dobutamine',      drugrate_adj, NULL)), 2) AS dobutamine,
    ROUND(MAX(IF(drug = 'phenylephrine',   drugrate_adj, NULL)), 2) AS phenylephrine
  FROM vaso_raw
  GROUP BY patientunitstayid
),
-- Vasopressin PRESENCE ONLY (binary), captured separately (units vary)
vasopressin_any AS (
  SELECT DISTINCT i.patientunitstayid, 1 AS vasopressin_flag
  FROM `physionet-data.eicu_crd.infusiondrug` AS i
  WHERE i.infusionoffset BETWEEN 0 AND 1440
    AND LOWER(i.drugname) LIKE '%vasopressin%'
),
map_values AS (
  SELECT
    v.patientunitstayid,
    CASE
      WHEN MIN(v.ibp_mean)  IS NOT NULL THEN MIN(v.ibp_mean)
      WHEN MIN(v.nibp_mean) IS NOT NULL THEN MIN(v.nibp_mean)
      ELSE NULL  -- impute conservatively downstream
    END AS map_min
  FROM `physionet-data.eicu_crd_derived.pivoted_vital` AS v
  WHERE v.entryoffset BETWEEN 0 AND 1440
  GROUP BY v.patientunitstayid
),
midodrine_flag AS (
  SELECT patientunitstayid, 1 AS midodrine
  FROM `physionet-data.eicu_crd.medication`
  WHERE LOWER(drugname) LIKE '%midodrine%'
    AND drugstartoffset BETWEEN 0 AND 1440
  GROUP BY patientunitstayid
)
SELECT
  m.patientunitstayid,
  m.age,
  m.gender,
  m.length_of_stay,
  m.mortality_30day,
  v.norepinephrine,
  v.epinephrine,
  v.dopamine,
  v.dobutamine,
  v.phenylephrine,
  COALESCE(vp.vasopressin_flag, 0) AS vasopressin_flag,
  COALESCE(mp.map_min, 71.0)       AS map,      -- conservative imputation for downstream no-vaso branch
  COALESCE(md.midodrine, 0)        AS midodrine
FROM mortality AS m
LEFT JOIN vaso           AS v  ON v.patientunitstayid  = m.patientunitstayid
LEFT JOIN vasopressin_any AS vp ON vp.patientunitstayid = m.patientunitstayid
LEFT JOIN map_values     AS mp ON mp.patientunitstayid = m.patientunitstayid
LEFT JOIN midodrine_flag AS md ON md.patientunitstayid = m.patientunitstayid
ORDER BY m.patientunitstayid;