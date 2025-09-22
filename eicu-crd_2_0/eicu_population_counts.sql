-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.eicu_population_counts

-- Purpose
--   Stepwise counts for eICU using PATIENT for S0–S2,
--   and using ICUSTAY_DETAIL (derived) for S3 to align with downstream day-1 features.
--   This makes S3 match analysis figures/tables based on icustay_detail (e.g., 157,801).

-- Sources
--   physionet-data.eicu_crd.patient                  -- ICU-stay level demographics
--   physionet-data.eicu_crd_derived.icustay_detail   -- derived day-1 features & metadata

-- Adult definition:
--   age >= 18 or age reported as '> 89' (treated as 90 for numeric checks).

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_population_counts AS
WITH
base_patient AS (
  SELECT
    p.patientunitstayid,
    p.patienthealthsystemstayid,
    p.gender,
    p.age,
    p.unitvisitnumber,
    CASE WHEN p.age = '> 89' THEN 90 ELSE SAFE_CAST(p.age AS INT64) END AS age_norm
  FROM `physionet-data.eicu_crd.patient` AS p
),

-- S0: all ICU stays (rows in PATIENT)
s0_all AS (
  SELECT COUNT(DISTINCT patientunitstayid) AS n
  FROM base_patient
),

-- S1: ICU stays with age AND sex present
s1_demo AS (
  SELECT COUNT(DISTINCT patientunitstayid) AS n
  FROM base_patient
  WHERE age IS NOT NULL
    AND gender IS NOT NULL
),

-- S2: Adult ICU stays with complete age/sex
s2_adults AS (
  SELECT COUNT(DISTINCT patientunitstayid) AS n
  FROM base_patient
  WHERE age IS NOT NULL
    AND gender IS NOT NULL
    AND ( (age != '> 89' AND age_norm >= 18) OR age = '> 89' )
),

-- S3: Adults with FIRST ICU stay only, aligned to icustay_detail
--     (match downstream analysis base: age/gender present, adult, unitvisitnumber = 1)
s3_first_icustay AS (
  SELECT COUNT(DISTINCT d.patientunitstayid) AS n
  FROM `physionet-data.eicu_crd_derived.icustay_detail` AS d
  WHERE d.age    IS NOT NULL
    AND d.gender IS NOT NULL
    AND ( (d.age != '> 89' AND SAFE_CAST(d.age AS INT64) >= 18) OR d.age = '> 89' )
    AND d.unitvisitnumber = 1
)

SELECT 'S0_all_icu_stays'         AS stage, "All ICU stays (rows in PATIENT)"                                      AS description, n FROM s0_all
UNION ALL
SELECT 'S1_complete_age_sex'      AS stage, "ICU stays with non-missing age & sex"                                  AS description, n FROM s1_demo
UNION ALL
SELECT 'S2_adults_age_ge_18'      AS stage, "Adult ICU stays (age >= 18 or age = '> 89') with complete demo"        AS description, n FROM s2_adults
UNION ALL
SELECT 'S3_adults_first_icu_only' AS stage, "Adults with FIRST ICU stay only (aligned to icustay_detail base)"      AS description, n FROM s3_first_icustay
ORDER BY stage;
