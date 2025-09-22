-- =====================================================================
-- File: eicu_charlson_map.sql
-- Dialect: BigQuery Standard SQL
-- Output: <PROJECT>.<DATASET>.eicu_charlson_map
--
-- Purpose
--   Build a reproducible mapping table from eICU `pasthistory` entries to
--   Charlson comorbidity categories. Each unique combination of
--   (pasthistorypath, pasthistoryvalue, pasthistoryvaluetext) is mapped to
--   one Charlson category and aggregated with the number of adult patients
--   (first ICU stay per subject) who have that entry.
--
-- Cohort
--   Adults (>=18; '> 89' treated as 90) and FIRST ICU unit visit only.
--
-- Inputs
--   physionet-data.eicu_crd_derived.icustay_detail
--   physionet-data.eicu_crd.pasthistory
--
-- Output columns
--   pasthistorypath         STRING
--   pasthistoryvalue        STRING
--   pasthistoryvaluetext    STRING
--   n_patients              INT64   -- distinct patients having the entry
--   charlson_category       STRING  -- mapped Charlson category (nullable)
--
-- Notes
--   • This table is a *mapping summary*; it does NOT compute a patient-level
--     Charlson score. It only shows which past-history entries map to which
--     Charlson category and how many patients had each entry.
--   • The mapping list below is a verbatim, one-by-one curation aligned with
--     your original public draft so downstream scripts can reuse it reliably.
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_charlson_map AS

WITH base AS (
  SELECT
    icu.patientunitstayid,
    CASE WHEN icu.age = '> 89' THEN 90 ELSE SAFE_CAST(icu.age AS INT64) END AS age,
    icu.gender,
    -- 30-day mortality and LOS kept for cohort sanity checks; not used downstream
    CASE
      WHEN icu.hospitaldischargeoffset BETWEEN 0 AND 43200 AND icu.hosp_mort = 1 THEN 1
      ELSE 0
    END AS mortality_30day,
    ROUND(icu.icu_los_hours / 24.0, 2) AS LOS
  FROM `physionet-data.eicu_crd_derived.icustay_detail` icu
  WHERE icu.age IS NOT NULL
    AND icu.gender IS NOT NULL
    AND icu.unitvisitnumber = 1
    AND (icu.age = '> 89' OR SAFE_CAST(icu.age AS INT64) >= 18)
),

hist_distinct AS (
  SELECT DISTINCT
    p.patientunitstayid,
    p.pasthistorypath,
    p.pasthistoryvalue,
    p.pasthistoryvaluetext
  FROM `physionet-data.eicu_crd.pasthistory` p
),

-- Verbatim mapping list from pasthistory → Charlson category
-- (kept line-by-line for transparency and maintainability)
mapping AS (
  SELECT * FROM UNNEST([
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Infectious Disease (R)/HIV (only)/HIV positive' AS pasthistorypath, 'HIV positive' AS pasthistoryvalue, 'HIV positive' AS pasthistoryvaluetext, 'aids' AS charlson_category),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Infectious Disease (R)/AIDS/AIDS', 'AIDS', 'AIDS', 'aids'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/Strokes/stroke - date unknown', 'stroke - date unknown', 'stroke - date unknown', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/TIA(s)/TIA(s) - date unknown', 'TIA(s) - date unknown', 'TIA(s) - date unknown', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/Strokes/stroke - within 6 months', 'stroke - within 6 months', 'stroke - within 6 months', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/Strokes/stroke - remote', 'stroke - remote', 'stroke - remote', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/Strokes/stroke - within 2 years', 'stroke - within 2 years', 'stroke - within 2 years', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/Strokes/stroke - within 5 years', 'stroke - within 5 years', 'stroke - within 5 years', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/Strokes/multiple/multiple', 'multiple', 'multiple', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/TIA(s)/TIA(s) - remote', 'TIA(s) - remote', 'TIA(s) - remote', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/TIA(s)/TIA(s) - within 6 months', 'TIA(s) - within 6 months', 'TIA(s) - within 6 months', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/TIA(s)/multiple/multiple', 'multiple', 'multiple', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/TIA(s)/TIA(s) - within 2 years', 'TIA(s) - within 2 years', 'TIA(s) - within 2 years', 'cerebrovascular_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/TIA(s)/TIA(s) - within 5 years', 'TIA(s) - within 5 years', 'TIA(s) - within 5 years', 'cerebrovascular_disease'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Pulmonary/COPD/COPD  - moderate', 'COPD  - moderate', 'COPD  - moderate', 'chronic_pulmonary_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Pulmonary/Asthma/asthma', 'asthma', 'asthma', 'chronic_pulmonary_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Pulmonary/COPD/COPD  - no limitations', 'COPD  - no limitations', 'COPD  - no limitations', 'chronic_pulmonary_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Pulmonary/COPD/COPD  - severe', 'COPD  - severe', 'COPD  - severe', 'chronic_pulmonary_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Pulmonary/Restrictive Disease/restrictive pulmonary disease', 'restrictive pulmonary disease', 'restrictive pulmonary disease', 'chronic_pulmonary_disease'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Congestive Heart Failure/CHF', 'CHF', 'CHF', 'congestive_heart_failure'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Congestive Heart Failure/CHF - severity unknown', 'CHF - severity unknown', 'CHF - severity unknown', 'congestive_heart_failure'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Congestive Heart Failure/CHF - class III', 'CHF - class III', 'CHF - class III', 'congestive_heart_failure'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Congestive Heart Failure/CHF - class IV', 'CHF - class IV', 'CHF - class IV', 'congestive_heart_failure'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Congestive Heart Failure/CHF - class II', 'CHF - class II', 'CHF - class II', 'congestive_heart_failure'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Congestive Heart Failure/CHF - class I', 'CHF - class I', 'CHF - class I', 'congestive_heart_failure'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Neurologic/Dementia/dementia', 'dementia', 'dementia', 'dementia'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Endocrine (R)/Insulin Dependent Diabetes/insulin dependent diabetes', 'insulin dependent diabetes', 'insulin dependent diabetes', 'diabetes_without_cc'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Endocrine (R)/Non-Insulin Dependent Diabetes/medication dependent', 'medication dependent', 'medication dependent', 'diabetes_without_cc'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Endocrine (R)/Non-Insulin Dependent Diabetes/non-medication dependent', 'non-medication dependent', 'non-medication dependent', 'diabetes_without_cc'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/breast', 'breast', 'breast', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/lung', 'lung', 'lung', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/prostate', 'prostate', 'prostate', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/colon', 'colon', 'colon', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/bladder', 'bladder', 'bladder', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/head and neck', 'head and neck', 'head and neck', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Hematologic Malignancy/non-Hodgkins lymphoma', 'non-Hodgkins lymphoma', 'non-Hodgkins lymphoma', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/kidney', 'kidney', 'kidney', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/melanoma', 'melanoma', 'melanoma', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/uterus', 'uterus', 'uterus', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/esophagus', 'esophagus', 'esophagus', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/pancreas - adenocarcinoma', 'pancreas - adenocarcinoma', 'pancreas - adenocarcinoma', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/brain', 'brain', 'brain', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/ovary', 'ovary', 'ovary', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Hematologic Malignancy/CLL', 'CLL', 'CLL', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/liver', 'liver', 'liver', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Hematologic Malignancy/other hematologic malignancy', 'other hematologic malignancy', 'other hematologic malignancy', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Hematologic Malignancy/Hodgkins disease', 'Hodgkins disease', 'Hodgkins disease', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Hematologic Malignancy/AML', 'AML', 'AML', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/stomach', 'stomach', 'stomach', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Hematologic Malignancy/leukemia - other', 'leukemia - other', 'leukemia - other', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/unknown', 'unknown', 'unknown', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Hematologic Malignancy/CML', 'CML', 'CML', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/testes', 'testes', 'testes', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/bone', 'bone', 'bone', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Hematologic Malignancy/ALL', 'ALL', 'ALL', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/bile duct', 'bile duct', 'bile duct', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/sarcoma', 'sarcoma', 'sarcoma', 'malignant_cancer'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Cancer-Primary Site/pancreas - islet cell', 'pancreas - islet cell', 'pancreas - islet cell', 'malignant_cancer'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Metastases/other', 'other', 'other', 'metastatic_solid_tumor'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Metastases/lung', 'lung', 'lung', 'metastatic_solid_tumor'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Metastases/liver', 'liver', 'liver', 'metastatic_solid_tumor'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Metastases/bone', 'bone', 'bone', 'metastatic_solid_tumor'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Metastases/brain', 'brain', 'brain', 'metastatic_solid_tumor'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Metastases/nodes', 'nodes', 'nodes', 'metastatic_solid_tumor'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Metastases/intra-abdominal', 'intra-abdominal', 'intra-abdominal', 'metastatic_solid_tumor'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Hematology/Oncology (R)/Cancer/Metastases/carcinomatosis', 'carcinomatosis', 'carcinomatosis', 'metastatic_solid_tumor'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Gastrointestinal (R)/Cirrhosis/clinical diagnosis', 'clinical diagnosis', 'clinical diagnosis', 'mild_liver_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Gastrointestinal (R)/Cirrhosis/biopsy proven', 'biopsy proven', 'biopsy proven', 'mild_liver_disease'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Myocardial Infarction/MI - date unknown', 'MI - date unknown', 'MI - date unknown', 'myocardial_infarct'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Myocardial Infarction/MI - remote', 'MI - remote', 'MI - remote', 'myocardial_infarct'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Myocardial Infarction/MI - within 6 months', 'MI - within 6 months', 'MI - within 6 months', 'myocardial_infarct'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Myocardial Infarction/MI - within 2 years', 'MI - within 2 years', 'MI - within 2 years', 'myocardial_infarct'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Myocardial Infarction/MI - within 5 years', 'MI - within 5 years', 'MI - within 5 years', 'myocardial_infarct'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Myocardial Infarction/multiple/multiple', 'multiple', 'multiple', 'myocardial_infarct'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Gastrointestinal (R)/Peptic Ulcer Disease/peptic ulcer disease', 'peptic ulcer disease', 'peptic ulcer disease', 'peptic_ulcer_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Gastrointestinal (R)/Peptic Ulcer Disease/peptic ulcer disease with h/o GI bleeding', 'peptic ulcer disease with h/o GI bleeding', 'peptic ulcer disease with h/o GI bleeding', 'peptic_ulcer_disease'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Cardiovascular (R)/Peripheral Vascular Disease/peripheral vascular disease', 'peripheral vascular disease', 'peripheral vascular disease', 'peripheral_vascular_disease'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Failure/renal failure - hemodialysis', 'renal failure - hemodialysis', 'renal failure - hemodialysis', 'renal_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Renal  (R)/s/p Renal Transplant/s/p renal transplant', 's/p renal transplant', 's/p renal transplant', 'renal_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Failure/renal failure - peritoneal dialysis', 'renal failure - peritoneal dialysis', 'renal failure - peritoneal dialysis', 'renal_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Insufficiency/renal insufficiency - creatinine 3-4', 'renal insufficiency - creatinine 3-4', 'renal insufficiency - creatinine 3-4', 'renal_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Insufficiency/renal insufficiency - creatinine > 5', 'renal insufficiency - creatinine > 5', 'renal insufficiency - creatinine > 5', 'renal_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Insufficiency/renal insufficiency - creatinine 4-5', 'renal insufficiency - creatinine 4-5', 'renal insufficiency - creatinine 4-5', 'renal_disease'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Rheumatic/Rheumatoid Arthritis/rheumatoid arthritis', 'rheumatoid arthritis', 'rheumatoid arthritis', 'rheumatic_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Rheumatic/SLE/SLE', 'SLE', 'SLE', 'rheumatic_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Rheumatic/Scleroderma/scleroderma', 'scleroderma', 'scleroderma', 'rheumatic_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Rheumatic/Vasculitis/vasculitis', 'vasculitis', 'vasculitis', 'rheumatic_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Rheumatic/Dermato/Polymyositis/dermatomyositis', 'dermatomyositis', 'dermatomyositis', 'rheumatic_disease'),

    STRUCT('notes/Progress Notes/Past History/Organ Systems/Gastrointestinal (R)/Cirrhosis/varices', 'varices', 'varices', 'severe_liver_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Gastrointestinal (R)/Cirrhosis/UGI bleeding', 'UGI bleeding', 'UGI bleeding', 'severe_liver_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Gastrointestinal (R)/Cirrhosis/ascites', 'ascites', 'ascites', 'severe_liver_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Gastrointestinal (R)/Cirrhosis/encephalopathy', 'encephalopathy', 'encephalopathy', 'severe_liver_disease'),
    STRUCT('notes/Progress Notes/Past History/Organ Systems/Gastrointestinal (R)/Cirrhosis/coma', 'coma', 'coma', 'severe_liver_disease')
  ])
)

SELECT
  COALESCE(h.pasthistorypath, 'NULL') AS pasthistorypath,
  COALESCE(CAST(h.pasthistoryvalue AS STRING), 'NULL') AS pasthistoryvalue,
  COALESCE(h.pasthistoryvaluetext, 'NULL') AS pasthistoryvaluetext,
  COUNT(DISTINCT b.patientunitstayid) AS n_patients,
  m.charlson_category AS charlson_category
FROM base b
JOIN hist_distinct h
  ON b.patientunitstayid = h.patientunitstayid
LEFT JOIN mapping m
  ON h.pasthistorypath = m.pasthistorypath
 AND CAST(h.pasthistoryvalue AS STRING) = m.pasthistoryvalue
 AND h.pasthistoryvaluetext = m.pasthistoryvaluetext
GROUP BY 1,2,3,5
ORDER BY n_patients DESC, pasthistorypath;
