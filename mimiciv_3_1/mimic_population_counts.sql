-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.mimic_population_counts
--
-- Purpose
--   Produce stepwise counts for the MIMIC-IV cohort:
--     (S0) All unique persons in HOSP.patients
--     (S1) Persons with age AND sex present
--     (S2) Adults only (age >= 18)
--     (S3) Adults with a FIRST ICU stay (one per subject)
--
-- Source tables
--   physionet-data.mimiciv_3_1_hosp.patients   (subject_id, gender, anchor_age)
--   physionet-data.mimiciv_3_1_icu.icustays    (subject_id, stay_id, intime, outtime)
--
-- Notes
--   * “FIRST ICU stay” = earliest ICU stay by intime per subject.
--   * Counts are DISTINCT by person (subject_id), not by ICU stays.
--   * Keep this file minimal and reproducible (README will reference it).
--
-- Style aligned with prior public scripts (see CV examples).  (ref) 
--   - conservative, explicit filters and windowing
--   - single output table for easy reporting/diffing
--   - English notes for GitHub users
-- (See also: mimic_asofa_cv.sql for conventions.)  -- :contentReference[oaicite:2]{index=2}

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.mimic_population_counts AS
WITH
patients AS (
  SELECT
    p.subject_id,
    p.gender,
    p.anchor_age
  FROM `physionet-data.mimiciv_3_1_hosp.patients` AS p
),
-- S0: all unique persons
s0_all AS (
  SELECT COUNT(DISTINCT subject_id) AS n
  FROM patients
),
-- S1: persons with age AND sex present
s1_demo AS (
  SELECT COUNT(DISTINCT subject_id) AS n
  FROM patients
  WHERE anchor_age IS NOT NULL
    AND gender     IS NOT NULL
),
-- S2: adults (age >= 18), still person-level
s2_adults AS (
  SELECT COUNT(DISTINCT subject_id) AS n
  FROM patients
  WHERE anchor_age IS NOT NULL
    AND gender     IS NOT NULL
    AND anchor_age >= 18
),
-- S3: adults with a FIRST ICU stay only (one per subject)
first_icu AS (
  SELECT
    i.subject_id,
    i.stay_id,
    ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime) AS rn
  FROM `physionet-data.mimiciv_3_1_icu.icustays` AS i
  -- join on adults with complete demographics
  JOIN patients AS p
    ON p.subject_id = i.subject_id
  WHERE p.anchor_age IS NOT NULL
    AND p.gender     IS NOT NULL
    AND p.anchor_age >= 18
),
s3_first_icu AS (
  SELECT COUNT(DISTINCT subject_id) AS n
  FROM first_icu
  WHERE rn = 1
)
SELECT 'S0_all_persons'                  AS stage, 'All persons in PATIENTS'                                    AS description, n FROM s0_all
UNION ALL
SELECT 'S1_complete_age_sex'             AS stage, 'Persons with non-missing age & sex'                         AS description, n FROM s1_demo
UNION ALL
SELECT 'S2_adults_age_ge_18'             AS stage, 'Adults (age >= 18) among persons with complete age & sex'   AS description, n FROM s2_adults
UNION ALL
SELECT 'S3_adults_first_icu_only'        AS stage, 'Adults with FIRST ICU stay (one per subject)'               AS description, n FROM s3_first_icu
ORDER BY stage;
