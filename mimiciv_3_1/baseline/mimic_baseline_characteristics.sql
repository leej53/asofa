-- =====================================================================
-- File: mimic_baseline_characteristics.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.mimic_baseline_characteristics
--
-- Purpose
--   Build a baseline characteristics table for the MIMIC cohort, combining
--   demographics (age, gender), total aSOFA/SOFA scores, LOS, and detailed
--   Charlson comorbidity components at the HADM level.
--
-- Alignment & Notes
--   * Source table (<PROJECT>.<DATASET>.mimic_asofa_total) provides
--     aSOFA/SOFA totals and LOS for adult FIRST ICU stays.
--   * The Charlson comorbidity data are joined from
--     physionet-data.mimiciv_3_1_derived.charlson at the (subject_id, hadm_id)
--     level, and the reported `charlson_comorbidity_index` here is the
--     **age-adjusted CCI**.
--   * Gender is encoded as 1=male and 0=otherwise to match other public scripts.
--   * For mutually exclusive Charlson pairs, we apply precedence rules:
--       severe_liver_disease → overrides mild_liver_disease
--       diabetes_with_cc     → overrides diabetes_without_cc
--       metastatic_solid_tumor → overrides malignant_cancer
--
-- Inputs
--   <PROJECT>.<DATASET>.mimic_asofa_total
--   physionet-data.mimiciv_3_1_hosp.patients
--   physionet-data.mimiciv_3_1_icu.icustays
--   physionet-data.mimiciv_3_1_derived.charlson
--
-- Output columns
--   subject_id | stay_id | age | gender(1=male,0=otherwise) | length_of_stay
--   asofa_total_score | sofa_total_score | charlson_comorbidity_index
--   hadm_id
--   (individual Charlson component flags...)
--
-- Versioning
--   GitHub-ready; headers and naming aligned with other public scripts.
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.mimic_baseline_characteristics AS
WITH base AS (
  -- Base cohort from the finalized totals table
  SELECT
    subject_id,
    stay_id,
    length_of_stay,
    asofa_total_score,
    sofa_total_score
  FROM `<PROJECT>.<DATASET>`.mimic_asofa_total
),

-- Patients: encode male as 1, others as 0; age from anchor_age
pats AS (
  SELECT
    subject_id,
    CAST(anchor_age AS INT64) AS age,
    CASE WHEN gender = 'M' THEN 1 ELSE 0 END AS gender  -- 1=male, 0=otherwise
  FROM `physionet-data.mimiciv_3_1_hosp.patients`
),

-- Map stay_id → hadm_id for rows present in base
stay2hadm AS (
  SELECT i.subject_id, i.stay_id, i.hadm_id
  FROM `physionet-data.mimiciv_3_1_icu.icustays` AS i
  JOIN base AS b USING (subject_id, stay_id)
),

-- Charlson raw flags and (age-adjusted) index at HADM level
charlson_raw AS (
  SELECT
    c.subject_id,
    c.hadm_id,
    MAX(CAST(c.myocardial_infarct            AS INT64)) AS myocardial_infarct,
    MAX(CAST(c.congestive_heart_failure      AS INT64)) AS congestive_heart_failure,
    MAX(CAST(c.peripheral_vascular_disease   AS INT64)) AS peripheral_vascular_disease,
    MAX(CAST(c.cerebrovascular_disease       AS INT64)) AS cerebrovascular_disease,
    MAX(CAST(c.dementia                      AS INT64)) AS dementia,
    MAX(CAST(c.chronic_pulmonary_disease     AS INT64)) AS chronic_pulmonary_disease,
    MAX(CAST(c.rheumatic_disease             AS INT64)) AS rheumatic_disease,
    MAX(CAST(c.peptic_ulcer_disease          AS INT64)) AS peptic_ulcer_disease,
    MAX(CAST(c.mild_liver_disease            AS INT64)) AS mild_liver_disease_raw,
    MAX(CAST(c.severe_liver_disease          AS INT64)) AS severe_liver_disease_raw,
    MAX(CAST(c.diabetes_without_cc           AS INT64)) AS diabetes_without_cc_raw,
    MAX(CAST(c.diabetes_with_cc              AS INT64)) AS diabetes_with_cc_raw,
    MAX(CAST(c.paraplegia                    AS INT64)) AS paraplegia,
    MAX(CAST(c.renal_disease                 AS INT64)) AS renal_disease,
    MAX(CAST(c.malignant_cancer              AS INT64)) AS malignant_cancer_raw,
    MAX(CAST(c.metastatic_solid_tumor        AS INT64)) AS metastatic_solid_tumor,
    MAX(CAST(c.aids                          AS INT64)) AS aids,
    -- Age-adjusted CCI brought from the derived view
    MAX(CAST(c.charlson_comorbidity_index    AS INT64)) AS charlson_comorbidity_index
  FROM `physionet-data.mimiciv_3_1_derived.charlson` AS c
  JOIN stay2hadm AS s
    ON c.subject_id = s.subject_id
   AND c.hadm_id    = s.hadm_id
  GROUP BY c.subject_id, c.hadm_id
),

-- Apply precedence (severe>mild liver; diabetes with cc>without; metastatic>malignant)
charlson_adj AS (
  SELECT
    subject_id,
    hadm_id,
    myocardial_infarct,
    congestive_heart_failure,
    peripheral_vascular_disease,
    cerebrovascular_disease,
    dementia,
    chronic_pulmonary_disease,
    rheumatic_disease,
    peptic_ulcer_disease,
    CASE WHEN severe_liver_disease_raw = 1 THEN 0 ELSE mild_liver_disease_raw END AS mild_liver_disease,
    severe_liver_disease_raw AS severe_liver_disease,
    CASE WHEN diabetes_with_cc_raw = 1 THEN 0 ELSE diabetes_without_cc_raw END AS diabetes_without_cc,
    diabetes_with_cc_raw AS diabetes_with_cc,
    paraplegia,
    renal_disease,
    CASE WHEN metastatic_solid_tumor = 1 THEN 0 ELSE malignant_cancer_raw END AS malignant_cancer,
    metastatic_solid_tumor,
    aids,
    charlson_comorbidity_index  -- keep the (age-adjusted) index
  FROM charlson_raw
)

SELECT
  b.subject_id,
  b.stay_id,

  -- Demographics & LOS
  p.age,
  p.gender,  -- 1 = male, 0 = otherwise
  b.length_of_stay,

  -- Scores
  b.asofa_total_score,
  b.sofa_total_score,

  -- Charlson (age-adjusted) index
  ca.charlson_comorbidity_index,

  -- Hospital admission identifier for linkage/QC
  s.hadm_id,

  -- Individual Charlson components (0/1; default 0 when absent)
  COALESCE(ca.myocardial_infarct,           0) AS myocardial_infarct,
  COALESCE(ca.congestive_heart_failure,     0) AS congestive_heart_failure,
  COALESCE(ca.peripheral_vascular_disease,  0) AS peripheral_vascular_disease,
  COALESCE(ca.cerebrovascular_disease,      0) AS cerebrovascular_disease,
  COALESCE(ca.dementia,                     0) AS dementia,
  COALESCE(ca.chronic_pulmonary_disease,    0) AS chronic_pulmonary_disease,
  COALESCE(ca.rheumatic_disease,            0) AS rheumatic_disease,
  COALESCE(ca.peptic_ulcer_disease,         0) AS peptic_ulcer_disease,
  COALESCE(ca.mild_liver_disease,           0) AS mild_liver_disease,
  COALESCE(ca.severe_liver_disease,         0) AS severe_liver_disease,
  COALESCE(ca.diabetes_without_cc,          0) AS diabetes_without_cc,
  COALESCE(ca.diabetes_with_cc,             0) AS diabetes_with_cc,
  COALESCE(ca.paraplegia,                   0) AS paraplegia,
  COALESCE(ca.renal_disease,                0) AS renal_disease,
  COALESCE(ca.malignant_cancer,             0) AS malignant_cancer,
  COALESCE(ca.metastatic_solid_tumor,       0) AS metastatic_solid_tumor,
  COALESCE(ca.aids,                         0) AS aids

FROM base AS b
LEFT JOIN pats        AS p USING (subject_id)
LEFT JOIN stay2hadm   AS s USING (subject_id, stay_id)
LEFT JOIN charlson_adj AS ca
  ON ca.subject_id = s.subject_id
 AND ca.hadm_id    = s.hadm_id
ORDER BY b.subject_id, b.stay_id;