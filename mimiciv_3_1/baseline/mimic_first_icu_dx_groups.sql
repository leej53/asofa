-- =====================================================================
-- File: mimic_first_icu_dx_groups.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.mimic_first_icu_dx_groups
--
-- Purpose
--   Generate diagnosis group distributions for the MIMIC-IV v3.1 cohort,
--   restricted to each subject’s FIRST ICU admission (hadm_id).
--   Ten broad diagnostic categories are flagged based on ICD-9/ICD-10 codes.
--
-- Cohort
--   * Adult patients, first ICU stay per subject.
--   * Each HADM may map to multiple diagnosis groups (percentages may sum >100).
--
-- Diagnosis groups (flagged from diagnoses_icd):
--   1) Sepsis/Septic shock  (extended definition: ICD-10 organism-specified codes)
--   2) Respiratory failure / ARDS
--   3) Pneumonia
--   4) Acute myocardial infarction
--   5) Heart failure
--   6) Stroke / Intracranial hemorrhage
--   7) Acute kidney injury
--   8) COPD / Asthma
--   9) Trauma
--   10) Post-op / Surgical complications
--
-- Notes
--   * Sepsis ICD definitions (methods): ICD-9 038*, 99591, 99592, 78552;
--     ICD-10 A40*, A41*, R6520, R6521, A021, A227, A267, A327, A427.
--   * Percent denominators = total distinct subjects in the first ICU cohort.
--   * Table is GitHub-ready and aligned with other public MIMIC scripts.
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.mimic_first_icu_dx_groups AS
WITH first_icu AS (
  SELECT subject_id, hadm_id,
         ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
  FROM `physionet-data.mimiciv_3_1_icu.icustays`
),
first_stays AS (
  SELECT subject_id, hadm_id
  FROM first_icu
  WHERE rn = 1
),
cohort AS (
  SELECT COUNT(DISTINCT subject_id) AS N
  FROM first_stays
),
dx AS (
  SELECT
    fs.subject_id,
    fs.hadm_id,
    d.icd_version,
    REGEXP_REPLACE(UPPER(d.icd_code), r'\.', '') AS icd_code
  FROM first_stays fs
  JOIN `physionet-data.mimiciv_3_1_hosp.diagnoses_icd` d
    ON fs.hadm_id = d.hadm_id
),
flags AS (
  SELECT
    subject_id,
    hadm_id,

    -- 1) Sepsis / Septic shock
    MAX(
      CASE
        WHEN icd_version = 9  AND (icd_code LIKE '038%' OR icd_code IN ('99591','99592','78552')) THEN 1
        WHEN icd_version = 10 AND (
               icd_code LIKE 'A40%' OR icd_code LIKE 'A41%'
            OR icd_code IN ('R6520','R6521','A021','A227','A267','A327','A427')
        ) THEN 1
        ELSE 0
      END) AS sepsis,

    -- 2) Respiratory failure / ARDS
    MAX(
      CASE
        WHEN icd_version = 9  AND (icd_code LIKE '51881%' OR icd_code LIKE '51882%' OR icd_code LIKE '51884%') THEN 1
        WHEN icd_version = 10 AND (icd_code LIKE 'J96%' OR icd_code LIKE 'J80%') THEN 1
        ELSE 0
      END) AS resp_fail_ards,

    -- 3) Pneumonia
    MAX(
      CASE
        WHEN icd_version = 9  AND (icd_code LIKE '480%' OR icd_code LIKE '481%' OR icd_code LIKE '482%' OR
                                   icd_code LIKE '483%' OR icd_code LIKE '484%' OR icd_code LIKE '485%' OR icd_code LIKE '486%') THEN 1
        WHEN icd_version = 10 AND (icd_code LIKE 'J12%' OR icd_code LIKE 'J13%' OR icd_code LIKE 'J14%' OR
                                   icd_code LIKE 'J15%' OR icd_code LIKE 'J16%' OR icd_code LIKE 'J17%' OR icd_code LIKE 'J18%') THEN 1
        ELSE 0
      END) AS pneumonia,

    -- 4) Acute myocardial infarction
    MAX(
      CASE
        WHEN icd_version = 9  AND icd_code LIKE '410%' THEN 1
        WHEN icd_version = 10 AND (icd_code LIKE 'I21%' OR icd_code LIKE 'I22%') THEN 1
        ELSE 0
      END) AS acute_mi,

    -- 5) Heart failure
    MAX(
      CASE
        WHEN icd_version = 9  AND icd_code LIKE '428%' THEN 1
        WHEN icd_version = 10 AND icd_code LIKE 'I50%' THEN 1
        ELSE 0
      END) AS heart_failure,

    -- 6) Stroke / Intracranial hemorrhage
    MAX(
      CASE
        WHEN icd_version = 9  AND (icd_code LIKE '431%' OR icd_code LIKE '434%') THEN 1
        WHEN icd_version = 10 AND (icd_code LIKE 'I61%' OR icd_code LIKE 'I63%') THEN 1
        ELSE 0
      END) AS stroke_ich,

    -- 7) Acute kidney injury
    MAX(
      CASE
        WHEN icd_version = 9  AND icd_code LIKE '584%' THEN 1
        WHEN icd_version = 10 AND icd_code LIKE 'N17%' THEN 1
        ELSE 0
      END) AS aki,

    -- 8) COPD / Asthma
    MAX(
      CASE
        WHEN icd_version = 9  AND (icd_code LIKE '491%' OR icd_code LIKE '492%' OR icd_code LIKE '493%' OR icd_code = '496') THEN 1
        WHEN icd_version = 10 AND (icd_code LIKE 'J44%' OR icd_code LIKE 'J45%') THEN 1
        ELSE 0
      END) AS copd_asthma,

    -- 9) Trauma
    MAX(
      CASE
        WHEN icd_version = 9  AND REGEXP_CONTAINS(icd_code, r'^(8\d{2}|9[0-5]\d)') THEN 1
        WHEN icd_version = 10 AND (icd_code LIKE 'S%' OR icd_code LIKE 'T0%' OR icd_code LIKE 'T1%') THEN 1
        ELSE 0
      END) AS trauma,

    -- 10) Post-op / Surgical complications
    MAX(
      CASE
        WHEN icd_version = 9  AND icd_code LIKE '998%' THEN 1
        WHEN icd_version = 10 AND icd_code LIKE 'T81%' THEN 1
        ELSE 0
      END) AS postop_comp
  FROM dx
  GROUP BY subject_id, hadm_id
),
long AS (
  -- Reshape flags to long format and keep only flagged groups
  SELECT subject_id, hadm_id, g.label AS dx_group
  FROM flags,
  UNNEST([
    STRUCT('Sepsis/Septic shock'      AS label, sepsis          AS flag),
    STRUCT('Respiratory failure/ARDS' AS label, resp_fail_ards  AS flag),
    STRUCT('Pneumonia'                AS label, pneumonia       AS flag),
    STRUCT('Acute MI'                 AS label, acute_mi        AS flag),
    STRUCT('Heart failure'            AS label, heart_failure   AS flag),
    STRUCT('Stroke/ICH'               AS label, stroke_ich      AS flag),
    STRUCT('Acute kidney injury'      AS label, aki             AS flag),
    STRUCT('COPD/Asthma'              AS label, copd_asthma     AS flag),
    STRUCT('Trauma'                   AS label, trauma          AS flag),
    STRUCT('Post-op/Surgical'         AS label, postop_comp     AS flag)
  ]) AS g
  WHERE g.flag = 1
),
counts AS (
  SELECT dx_group, COUNT(DISTINCT subject_id) AS n_subjects
  FROM long
  GROUP BY dx_group
),
dx_groups AS (
  -- Reference list of 10 diagnosis groups
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
SELECT
  g.dx_group,
  COALESCE(c.n_subjects, 0) AS n_subjects,
  ROUND(100 * COALESCE(c.n_subjects, 0) / (SELECT N FROM cohort), 2) AS pct
FROM dx_groups g
LEFT JOIN counts c USING (dx_group)
ORDER BY n_subjects DESC, dx_group;
