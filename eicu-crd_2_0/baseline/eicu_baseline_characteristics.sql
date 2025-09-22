-- =====================================================================
-- File: eicu_baseline_characteristics.sql
-- Dialect: BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.eicu_baseline_characteristics
--
-- Purpose
--   Build a baseline characteristics table for the eICU cohort (adults,
--   FIRST ICU unit visit), combining demographics (age, gender), total
--   aSOFA/SOFA, LOS/mortality, and Charlson comorbidity components.
--
-- Cohort & Alignment
--   * Cohort definition, column naming, and header style align with the
--     finalized public eICU scripts. Baseline rows are taken from the
--     totals table <PROJECT>.<DATASET>.eicu_asofa_total (adult + first ICU
--     stay already applied). [ref]  <-- uses age/gender/LOS/outcomes
--   * Charlson components are derived from `pasthistory` via the curated
--     mapping table <PROJECT>.<DATASET>.eicu_charlson_map. [ref]
--
-- Charlson note
--   * We compute the **age-adjusted Charlson Comorbidity Index (CCI)**:
--       Age weights: 50–59=+1; 60–69=+2; 70–79=+3; ≥80=+4.
--       Component weights: MI/CHF/PVD/CVD/Dementia/COPD/Rheumatic/PUD=1;
--                          Diabetes (with complications)=2; (without)=1;
--                          Paraplegia=2; Renal disease=2; Malignant cancer=2;
--                          Moderate–severe liver=3; Metastatic solid tumor=6;
--                          AIDS=6.  Mutually exclusive precedence applied:
--                          severe_liver > mild_liver; dm_with_cc > dm_without_cc;
--                          metastatic > malignant.
--
-- Inputs
--   <PROJECT>.<DATASET>.eicu_asofa_total          -- base cohort & totals
--   <PROJECT>.<DATASET>.eicu_charlson_map         -- pasthistory→Charlson mapping (curated)
--   physionet-data.eicu_crd.pasthistory           -- raw past history
--
-- Output columns
--   patientunitstayid | age | gender(1=male,0=otherwise)
--   length_of_stay | mortality_30day
--   asofa_total_score | sofa_total_score
--   charlson_comorbidity_index
--   (individual Charlson component flags...)
--
-- Versioning
--   GitHub-ready; headers and naming aligned with other public scripts.
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_baseline_characteristics AS
WITH
-- ---------------------------------------------------------------------
-- 0) Base cohort and totals (adult + first ICU stay already applied)
-- ---------------------------------------------------------------------
base AS (
  SELECT
    patientunitstayid,
    gender,
    SAFE_CAST(age AS INT64) AS age,
    length_of_stay,
    mortality_30day,
    asofa_total_score,
    sofa_total_score
  FROM `<PROJECT>.<DATASET>`.eicu_asofa_total
),

-- ---------------------------------------------------------------------
-- 1) Distinct pasthistory entries for the cohort
-- ---------------------------------------------------------------------
ph AS (
  SELECT DISTINCT
    p.patientunitstayid,
    p.pasthistorypath,
    CAST(p.pasthistoryvalue AS STRING) AS pasthistoryvalue,
    p.pasthistoryvaluetext
  FROM `physionet-data.eicu_crd.pasthistory` p
  JOIN base b USING (patientunitstayid)
),

-- ---------------------------------------------------------------------
-- 2) Join curated mapping (verbatim mapping list maintained separately)
--    Only non-empty mapped categories are kept.
-- ---------------------------------------------------------------------
ph_mapped AS (
  SELECT DISTINCT
    ph.patientunitstayid,
    m.charlson_category
  FROM ph
  JOIN `<PROJECT>.<DATASET>`.eicu_charlson_map m
    ON COALESCE(ph.pasthistorypath,'')       = COALESCE(m.pasthistorypath,'')
   AND COALESCE(ph.pasthistoryvalue,'')      = COALESCE(m.pasthistoryvalue,'')
   AND COALESCE(ph.pasthistoryvaluetext,'')  = COALESCE(m.pasthistoryvaluetext,'')
  WHERE m.charlson_category IS NOT NULL AND m.charlson_category != ''
),

-- ---------------------------------------------------------------------
-- 3) Text probe to infer renal complications for diabetes (optional rule)
--    This mirrors the prior internal logic: "renal insufficien*" anywhere
--    in the concatenated past-history strings implies diabetes complications
--    when diabetes is present.
-- ---------------------------------------------------------------------
renal_insuff AS (
  SELECT
    ph.patientunitstayid,
    MAX(
      CASE WHEN REGEXP_CONTAINS(
        LOWER(CONCAT(
          COALESCE(ph.pasthistorypath,''),' ',
          COALESCE(ph.pasthistoryvalue,''),' ',
          COALESCE(ph.pasthistoryvaluetext,'')))
        , r'renal\s+insufficien') THEN 1 ELSE 0 END
    ) AS renal_insuff_flag
  FROM ph
  GROUP BY ph.patientunitstayid
),

-- ---------------------------------------------------------------------
-- 4) Patient-level raw flags from mapped categories
-- ---------------------------------------------------------------------
raw_flags AS (
  SELECT
    b.patientunitstayid,
    -- 1-point set
    MAX(CASE WHEN charlson_category = 'myocardial_infarct'          THEN 1 ELSE 0 END) AS has_mi,
    MAX(CASE WHEN charlson_category = 'congestive_heart_failure'    THEN 1 ELSE 0 END) AS has_chf,
    MAX(CASE WHEN charlson_category = 'peripheral_vascular_disease' THEN 1 ELSE 0 END) AS has_pvd,
    MAX(CASE WHEN charlson_category = 'cerebrovascular_disease'     THEN 1 ELSE 0 END) AS has_cvd,
    MAX(CASE WHEN charlson_category = 'dementia'                    THEN 1 ELSE 0 END) AS has_dementia,
    MAX(CASE WHEN charlson_category = 'chronic_pulmonary_disease'   THEN 1 ELSE 0 END) AS has_copd,
    MAX(CASE WHEN charlson_category = 'rheumatic_disease'           THEN 1 ELSE 0 END) AS has_rheum,
    MAX(CASE WHEN charlson_category = 'peptic_ulcer_disease'        THEN 1 ELSE 0 END) AS has_pud,
    MAX(CASE WHEN charlson_category = 'mild_liver_disease'          THEN 1 ELSE 0 END) AS has_liver_mild,
    MAX(CASE WHEN charlson_category = 'diabetes_without_cc'         THEN 1 ELSE 0 END) AS has_dm_nocc,
    -- higher-weight set
    MAX(CASE WHEN charlson_category = 'diabetes_with_cc'            THEN 1 ELSE 0 END) AS has_dm_cc,
    MAX(CASE WHEN charlson_category = 'paraplegia'                  THEN 1 ELSE 0 END) AS has_para,
    MAX(CASE WHEN charlson_category = 'renal_disease'               THEN 1 ELSE 0 END) AS has_renal,
    MAX(CASE WHEN charlson_category = 'malignant_cancer'            THEN 1 ELSE 0 END) AS has_malig,
    MAX(CASE WHEN charlson_category = 'severe_liver_disease'        THEN 1 ELSE 0 END) AS has_liver_severe,
    MAX(CASE WHEN charlson_category = 'metastatic_solid_tumor'      THEN 1 ELSE 0 END) AS has_met,
    MAX(CASE WHEN charlson_category = 'aids'                        THEN 1 ELSE 0 END) AS has_aids
  FROM base b
  LEFT JOIN ph_mapped m USING (patientunitstayid)
  GROUP BY b.patientunitstayid
),

-- ---------------------------------------------------------------------
-- 5) Apply precedence rules and convert to weighted Charlson components
-- ---------------------------------------------------------------------
charlson_weighted AS (
  SELECT
    b.patientunitstayid,
    -- precedence: severe > mild liver
    CASE WHEN f.has_liver_severe = 1 THEN 0 ELSE f.has_liver_mild END AS mild_liver_disease,      -- 1 point
    CASE WHEN f.has_liver_severe = 1 THEN 1 ELSE 0 END           AS severe_liver_disease_flag,    -- 3 points (weight applied below)
    -- precedence: metastatic > malignant cancer
    CASE WHEN f.has_met = 1 THEN 0 ELSE f.has_malig END          AS malignant_cancer_flag,        -- 2 points
    CASE WHEN f.has_met = 1 THEN 1 ELSE 0 END                    AS metastatic_solid_tumor_flag,  -- 6 points
    -- diabetes with complications overrides without
    CASE
      WHEN f.has_dm_cc = 1 OR (f.has_dm_nocc = 1 AND COALESCE(r.renal_insuff_flag,0) = 1) THEN 1 ELSE 0
    END AS diabetes_with_cc_flag,                                                                      -- 2 points
    CASE
      WHEN (f.has_dm_cc = 1 OR (f.has_dm_nocc = 1 AND COALESCE(r.renal_insuff_flag,0) = 1)) THEN 0
      WHEN f.has_dm_nocc = 1 THEN 1 ELSE 0
    END AS diabetes_without_cc_flag,                                                                    -- 1 point

    -- direct 1-point flags
    f.has_mi      AS myocardial_infarct,
    f.has_chf     AS congestive_heart_failure,
    f.has_pvd     AS peripheral_vascular_disease,
    f.has_cvd     AS cerebrovascular_disease,
    f.has_dementia AS dementia,
    f.has_copd    AS chronic_pulmonary_disease,
    f.has_rheum   AS rheumatic_disease,
    f.has_pud     AS peptic_ulcer_disease,

    -- higher-weight direct flags
    f.has_para  AS paraplegia,   -- 2 points
    f.has_renal AS renal_disease, -- 2 points
    f.has_aids  AS aids           -- 6 points
  FROM base b
  LEFT JOIN raw_flags f USING (patientunitstayid)
  LEFT JOIN renal_insuff r USING (patientunitstayid)
),

-- ---------------------------------------------------------------------
-- 6) Compute age-adjusted CCI
-- ---------------------------------------------------------------------
cci AS (
  SELECT
    b.patientunitstayid,
    -- Age weights
    CASE
      WHEN b.age >= 80 THEN 4
      WHEN b.age >= 70 THEN 3
      WHEN b.age >= 60 THEN 2
      WHEN b.age >= 50 THEN 1
      ELSE 0
    END AS age_score,

    -- Component weights
    (cw.myocardial_infarct
     + cw.congestive_heart_failure
     + cw.peripheral_vascular_disease
     + cw.cerebrovascular_disease
     + cw.dementia
     + cw.chronic_pulmonary_disease
     + cw.rheumatic_disease
     + cw.peptic_ulcer_disease
     + cw.diabetes_without_cc_flag
    ) AS cci_1pt_sum,

    2 * (cw.diabetes_with_cc_flag + cw.paraplegia + cw.renal_disease + cw.malignant_cancer_flag) AS cci_2pt_sum,
    3 * (cw.severe_liver_disease_flag) AS cci_3pt_sum,
    6 * (cw.metastatic_solid_tumor_flag + cw.aids) AS cci_6pt_sum
  FROM base b
  LEFT JOIN charlson_weighted cw USING (patientunitstayid)
),

charlson_final AS (
  SELECT
    cw.*,
    c.age_score
      + c.cci_1pt_sum
      + c.cci_2pt_sum
      + c.cci_3pt_sum
      + c.cci_6pt_sum AS charlson_comorbidity_index
  FROM charlson_weighted cw
  LEFT JOIN cci c USING (patientunitstayid)
)

-- ---------------------------------------------------------------------
-- 7) Final baseline table
-- ---------------------------------------------------------------------
SELECT
  b.patientunitstayid,
  b.age,
  b.gender,  -- 1=male, 0=otherwise
  b.length_of_stay,
  b.mortality_30day,
  b.asofa_total_score,
  b.sofa_total_score,

  cf.charlson_comorbidity_index,

  -- Individual Charlson components as 0/1 flags
  COALESCE(cf.myocardial_infarct,           0) AS myocardial_infarct,
  COALESCE(cf.congestive_heart_failure,     0) AS congestive_heart_failure,
  COALESCE(cf.peripheral_vascular_disease,  0) AS peripheral_vascular_disease,
  COALESCE(cf.cerebrovascular_disease,      0) AS cerebrovascular_disease,
  COALESCE(cf.dementia,                     0) AS dementia,
  COALESCE(cf.chronic_pulmonary_disease,    0) AS chronic_pulmonary_disease,
  COALESCE(cf.rheumatic_disease,            0) AS rheumatic_disease,
  COALESCE(cf.peptic_ulcer_disease,         0) AS peptic_ulcer_disease,
  COALESCE(cf.mild_liver_disease,           0) AS mild_liver_disease,
  COALESCE(cf.severe_liver_disease_flag,    0) AS severe_liver_disease,
  COALESCE(cf.diabetes_without_cc_flag,     0) AS diabetes_without_cc,
  COALESCE(cf.diabetes_with_cc_flag,        0) AS diabetes_with_cc,
  COALESCE(cf.paraplegia,                   0) AS paraplegia,
  COALESCE(cf.renal_disease,                0) AS renal_disease,
  COALESCE(cf.malignant_cancer_flag,        0) AS malignant_cancer,
  COALESCE(cf.metastatic_solid_tumor_flag,  0) AS metastatic_solid_tumor,
  COALESCE(cf.aids,                         0) AS aids

FROM base b
LEFT JOIN charlson_final cf USING (patientunitstayid)
ORDER BY b.patientunitstayid;
