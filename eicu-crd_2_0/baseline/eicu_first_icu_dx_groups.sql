-- =====================================================================
-- File: eicu_first_icu_dx_groups.sql
-- Dialect: BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.eicu_first_icu_dx_groups
--
-- Purpose
--   Generate diagnosis group distributions for the eICU cohort, restricted
--   to each subject’s FIRST ICU unit visit (adults). Ten broad diagnostic
--   categories are flagged using ICD-9/ICD-10 codes from `eicu_crd.diagnosis`.
--
-- Cohort
--   * Adults (>=18; '> 89' treated as 90) and FIRST ICU unit visit only.
--   * Denominator (pct) = total distinct patientunitstayid in the cohort.
--
-- Diagnosis groups (flagged from ICD codes, with text fallback):
--   1) Sepsis/Septic shock        (ICD-9: 038*, 99591, 99592, 78552; ICD-10: A40*, A41*, R6520, R6521, A021, A227, A267, A327, A427)
--   2) Respiratory failure / ARDS (ICD-9: 51881/51882/51884; ICD-10: J96*, J80*)
--   3) Pneumonia                  (ICD-9: 480–486, 4870; ICD-10: J12–J18)
--   4) Acute myocardial infarction (ICD-9: 410*; ICD-10: I21*, I22*)
--   5) Heart failure              (ICD-9: 428*; ICD-10: I50*)
--   6) Stroke / Intracranial hemorrhage (ICD-9: 431*, 434*; ICD-10: I61*, I63*)
--   7) Acute kidney injury        (ICD-9: 584*; ICD-10: N17*)
--   8) COPD / Asthma              (ICD-9: 491*,492*,493*,496; ICD-10: J44*, J45*)
--   9) Trauma                     (ICD-9 injury ranges; ICD-10: S*, T0*, T1*)
--   10) Post-op / Surgical complications (ICD-9: 998*; ICD-10: T81*)
--
-- Notes
--   * `diagnosis.icd9code` may contain multiple codes separated by pipes/semicolons/
--     whitespaces; we normalize delimiters to commas, then UNNEST. Dots and non-
--     alphanumerics are removed before pattern matching to harmonize ICD-9/10.
--   * Rows without any codes fall back to keyword search on `diagnosisstring`.
--   * A patient can belong to multiple groups (percentages can sum >100%).
--   * Header style and naming match other public eICU scripts (eicu_asofa_total). 
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_first_icu_dx_groups AS
WITH
-- 1) Adult first-ICU cohort
base AS (
  SELECT icu.patientunitstayid
  FROM `physionet-data.eicu_crd_derived.icustay_detail` AS icu
  WHERE icu.age IS NOT NULL
    AND icu.gender IS NOT NULL
    AND icu.unitvisitnumber = 1
    AND (icu.age = '> 89' OR SAFE_CAST(icu.age AS INT64) >= 18)
),
cohort AS (
  SELECT COUNT(*) AS N FROM base
),

-- 2) Raw diagnosis rows (normalize multi-code string)
diag_raw AS (
  SELECT
    d.patientunitstayid,
    UPPER(IFNULL(d.diagnosisstring, '')) AS diagnosisstring_up,
    -- replace pipes/semicolons/multiple spaces with commas
    REGEXP_REPLACE(UPPER(IFNULL(d.icd9code, '')), r'[\s|;]+', ',') AS icd_codes_norm
  FROM `physionet-data.eicu_crd.diagnosis` d
  JOIN base b USING (patientunitstayid)
),

-- 3) Split & normalize codes ("J18.9"→"J189", "410.71"→"41071")
codes AS (
  SELECT
    patientunitstayid,
    REGEXP_REPLACE(TRIM(code), r'[^A-Z0-9]', '') AS icd_code,
    diagnosisstring_up AS diagnosisstring
  FROM diag_raw,
  UNNEST(SPLIT(icd_codes_norm, ',')) AS code
  WHERE TRIM(code) <> ''
),

-- 4) Backup rows where no codes existed (text-only)
text_only AS (
  SELECT DISTINCT
    patientunitstayid,
    CAST(NULL AS STRING) AS icd_code,
    diagnosisstring_up   AS diagnosisstring
  FROM diag_raw
  WHERE icd_codes_norm IS NULL OR icd_codes_norm = ''
),

-- 5) Union (code+text)
dx AS (
  SELECT patientunitstayid, icd_code, diagnosisstring FROM codes
  UNION ALL
  SELECT patientunitstayid, icd_code, diagnosisstring FROM text_only
),

-- 6) Patient-level flags (ICD first; fallback to diagnosisstring when code is NULL)
flags AS (
  SELECT
    patientunitstayid,

    -- 1) Sepsis / Septic shock
    MAX(CASE
          WHEN icd_code LIKE '038%' OR icd_code IN ('99591','99592','78552')
               OR icd_code LIKE 'A40%' OR icd_code LIKE 'A41%'
               OR icd_code IN ('R6520','R6521','A021','A227','A267','A327','A427')
          THEN 1
          WHEN icd_code IS NULL AND REGEXP_CONTAINS(diagnosisstring, r'\bSEPSIS\b|SEPTIC SHOCK|\bSEPTIC\b') THEN 1
          ELSE 0 END) AS sepsis,

    -- 2) Respiratory failure / ARDS
    MAX(CASE
          WHEN icd_code LIKE '51881%' OR icd_code LIKE '51882%' OR icd_code LIKE '51884%'
               OR icd_code LIKE 'J96%'  OR icd_code LIKE 'J80%'
          THEN 1
          WHEN icd_code IS NULL AND REGEXP_CONTAINS(diagnosisstring, r'ACUTE RESPIRATORY FAILURE|RESPIRATORY FAILURE\b|\bARF\b|ARDS') THEN 1
          ELSE 0 END) AS resp_fail_ards,

    -- 3) Pneumonia
    MAX(CASE
          WHEN REGEXP_CONTAINS(icd_code, r'^(480|481|482|483|484|485|486)\d*$')
               OR icd_code LIKE '4870%'
               OR icd_code LIKE 'J12%' OR icd_code LIKE 'J13%' OR icd_code LIKE 'J14%'
               OR icd_code LIKE 'J15%' OR icd_code LIKE 'J16%' OR icd_code LIKE 'J17%' OR icd_code LIKE 'J18%'
          THEN 1
          WHEN icd_code IS NULL AND REGEXP_CONTAINS(diagnosisstring, r'\bPNEUMONIA\b') THEN 1
          ELSE 0 END) AS pneumonia,

    -- 4) Acute myocardial infarction
    MAX(CASE
          WHEN icd_code LIKE '410%' OR icd_code LIKE 'I21%' OR icd_code LIKE 'I22%'
          THEN 1
          WHEN icd_code IS NULL AND REGEXP_CONTAINS(diagnosisstring, r'\bMYOCARDIAL INFARCT(ION)?\b|\bSTEMI\b|\bNSTEMI\b') THEN 1
          ELSE 0 END) AS acute_mi,

    -- 5) Heart failure
    MAX(CASE
          WHEN icd_code LIKE '428%' OR icd_code LIKE 'I50%'
          THEN 1
          WHEN icd_code IS NULL AND REGEXP_CONTAINS(diagnosisstring, r'\bHEART FAILURE\b|\bCHF\b') THEN 1
          ELSE 0 END) AS heart_failure,

    -- 6) Stroke / Intracranial hemorrhage
    MAX(CASE
          WHEN icd_code LIKE '431%' OR icd_code LIKE '434%' OR icd_code LIKE 'I61%' OR icd_code LIKE 'I63%'
          THEN 1
          WHEN icd_code IS NULL AND REGEXP_CONTAINS(diagnosisstring, r'\bSTROKE\b|INTRACRANIAL HEMORRHAGE|\bICH\b|CEREBRAL INFARCTION') THEN 1
          ELSE 0 END) AS stroke_ich,

    -- 7) Acute kidney injury
    MAX(CASE
          WHEN icd_code LIKE '584%' OR icd_code LIKE 'N17%'
          THEN 1
          WHEN icd_code IS NULL AND REGEXP_CONTAINS(diagnosisstring, r'\bAKI\b|ACUTE KIDNEY INJURY') THEN 1
          ELSE 0 END) AS aki,

    -- 8) COPD / Asthma
    MAX(CASE
          WHEN icd_code LIKE '491%' OR icd_code LIKE '492%' OR icd_code LIKE '493%' OR icd_code = '496'
               OR icd_code LIKE 'J44%' OR icd_code LIKE 'J45%'
          THEN 1
          WHEN icd_code IS NULL AND REGEXP_CONTAINS(diagnosisstring, r'\bCOPD\b|\bASTHMA\b') THEN 1
          ELSE 0 END) AS copd_asthma,

    -- 9) Trauma
    MAX(CASE
          WHEN REGEXP_CONTAINS(icd_code, r'^(8\d{2}|9[0-5]\d)\d*$')  -- ICD-9 injury ranges
               OR icd_code LIKE 'S%' OR icd_code LIKE 'T0%' OR icd_code LIKE 'T1%'
          THEN 1
          WHEN icd_code IS NULL AND REGEXP_CONTAINS(diagnosisstring, r'\bTRAUMA\b|FRACTURE|LACERATION|CONTUSION') THEN 1
          ELSE 0 END) AS trauma,

    -- 10) Post-op / Surgical complications
    MAX(CASE
          WHEN icd_code LIKE '998%' OR icd_code LIKE 'T81%'
          THEN 1
          WHEN icd_code IS NULL AND REGEXP_CONTAINS(diagnosisstring, r'POSTOP|POST\-OP|SURGICAL COMPLICATION') THEN 1
          ELSE 0 END) AS postop_comp
  FROM dx
  GROUP BY patientunitstayid
),

-- 7) Long table of flagged groups
long AS (
  SELECT patientunitstayid, g.label AS dx_group
  FROM flags,
  UNNEST([
    STRUCT('Sepsis/Septic shock'       AS label, sepsis         AS flag),
    STRUCT('Respiratory failure/ARDS'  AS label, resp_fail_ards AS flag),
    STRUCT('Pneumonia'                 AS label, pneumonia      AS flag),
    STRUCT('Acute MI'                  AS label, acute_mi       AS flag),
    STRUCT('Heart failure'             AS label, heart_failure  AS flag),
    STRUCT('Stroke/ICH'                AS label, stroke_ich     AS flag),
    STRUCT('Acute kidney injury'       AS label, aki            AS flag),
    STRUCT('COPD/Asthma'               AS label, copd_asthma    AS flag),
    STRUCT('Trauma'                    AS label, trauma         AS flag),
    STRUCT('Post-op/Surgical'          AS label, postop_comp    AS flag)
  ]) AS g
  WHERE g.flag = 1
),

-- 8) Count by group
counts AS (
  SELECT dx_group, COUNT(DISTINCT patientunitstayid) AS n_subjects
  FROM long
  GROUP BY dx_group
),

-- 9) Ensure stable ordering & include zero rows
dx_groups AS (
  SELECT dx_group FROM UNNEST([
    'Sepsis/Septic shock',
    'Respiratory failure/ARDS',
    'Pneumonia',
    'Acute MI',
    'Heart failure',
    'Stroke/ICH',
    'Acute kidney injury',
    'COPD/Asthma',
    'Trauma',
    'Post-op/Surgical'
  ]) AS dx_group
)

-- 10) Final output
SELECT
  g.dx_group,
  COALESCE(c.n_subjects, 0) AS n_subjects,
  ROUND(100 * COALESCE(c.n_subjects, 0) / (SELECT N FROM cohort), 2) AS pct
FROM dx_groups g
LEFT JOIN counts c USING (dx_group)
ORDER BY n_subjects DESC, dx_group;
