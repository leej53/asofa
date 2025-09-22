-- BigQuery Standard SQL
-- Output: <PROJECT>.<DATASET>.suspicion_of_infection
--
-- Purpose
--   Antibiotic–culture timing rule identical to your latest eICU approach:
--     culture → (≤72h) → antibiotic   OR   antibiotic → (≤24h) → culture
--   The "suspected_infection_time" is the earliest culture time (if before abx)
--   or the antibiotic time (if no prior culture). One row per antibiotic (ab_id).
--
-- Sources (MIMIC-IV v3.1):
--   physionet-data.mimiciv_3_1_derived.antibiotic
--   physionet-data.mimiciv_3_1_hosp.microbiologyevents
--
-- Notes
--   * We keep ab_id granularity; Sepsis-3 later picks the earliest event per stay.
--   * Columns: subject_id, stay_id, hadm_id, ab_id, antibiotic, antibiotic_time,
--              suspected_infection (0/1), suspected_infection_time, culture_time,
--              specimen, positive_culture.

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.suspicion_of_infection AS
WITH ab_tbl AS (
  SELECT
    abx.subject_id, abx.hadm_id, abx.stay_id,
    abx.antibiotic,
    abx.starttime AS antibiotic_time,
    DATETIME_TRUNC(abx.starttime, DAY) AS antibiotic_date,
    abx.stoptime AS antibiotic_stoptime,
    ROW_NUMBER() OVER (
      PARTITION BY abx.subject_id
      ORDER BY abx.starttime, abx.stoptime, abx.antibiotic
    ) AS ab_id
  FROM `physionet-data.mimiciv_3_1_derived.antibiotic` AS abx
),
me AS (
  SELECT
    micro_specimen_id,
    MAX(subject_id) AS subject_id,
    MAX(hadm_id) AS hadm_id,
    CAST(MAX(chartdate) AS DATE) AS chartdate,
    MAX(charttime) AS charttime,
    MAX(spec_type_desc) AS spec_type_desc,
    MAX(
      CASE
        WHEN org_name IS NOT NULL AND org_itemid != 90856 AND org_name != ''
        THEN 1 ELSE 0
      END
    ) AS positiveculture
  FROM `physionet-data.mimiciv_3_1_hosp.microbiologyevents`
  GROUP BY micro_specimen_id
),
me_then_ab AS (
  -- culture → (≤72h) → antibiotic
  SELECT
    a.subject_id, a.hadm_id, a.stay_id, a.ab_id,
    m.micro_specimen_id,
    COALESCE(m.charttime, DATETIME(m.chartdate)) AS last72_charttime,
    m.positiveculture AS last72_positiveculture,
    m.spec_type_desc AS last72_specimen,
    ROW_NUMBER() OVER (
      PARTITION BY a.subject_id, a.ab_id
      ORDER BY m.chartdate, m.charttime NULLS LAST
    ) AS micro_seq
  FROM ab_tbl a
  LEFT JOIN me m
    ON a.subject_id = m.subject_id
   AND (
     (m.charttime IS NOT NULL
       AND a.antibiotic_time > m.charttime
       AND a.antibiotic_time <= DATETIME_ADD(m.charttime, INTERVAL 72 HOUR))
     OR
     (m.charttime IS NULL
       AND a.antibiotic_date >= m.chartdate
       AND a.antibiotic_date <= DATE_ADD(m.chartdate, INTERVAL 3 DAY))
   )
),
ab_then_me AS (
  -- antibiotic → (≤24h) → culture
  SELECT
    a.subject_id, a.hadm_id, a.stay_id, a.ab_id,
    m.micro_specimen_id,
    COALESCE(m.charttime, DATETIME(m.chartdate)) AS next24_charttime,
    m.positiveculture AS next24_positiveculture,
    m.spec_type_desc AS next24_specimen,
    ROW_NUMBER() OVER (
      PARTITION BY a.subject_id, a.ab_id
      ORDER BY m.chartdate, m.charttime NULLS LAST
    ) AS micro_seq
  FROM ab_tbl a
  LEFT JOIN me m
    ON a.subject_id = m.subject_id
   AND (
     (m.charttime IS NOT NULL
       AND a.antibiotic_time >= DATETIME_SUB(m.charttime, INTERVAL 24 HOUR)
       AND a.antibiotic_time < m.charttime)
     OR
     (m.charttime IS NULL
       AND a.antibiotic_date >= DATE_SUB(m.chartdate, INTERVAL 1 DAY)
       AND a.antibiotic_date <= m.chartdate)
   )
)
SELECT
  a.subject_id,
  a.stay_id,
  a.hadm_id,
  a.ab_id,
  a.antibiotic,
  a.antibiotic_time,
  CASE WHEN last72_specimen IS NULL AND next24_specimen IS NULL THEN 0 ELSE 1 END AS suspected_infection,
  CASE
    WHEN last72_specimen IS NULL AND next24_specimen IS NULL THEN NULL
    ELSE COALESCE(last72_charttime, a.antibiotic_time)
  END AS suspected_infection_time,
  COALESCE(last72_charttime, next24_charttime) AS culture_time,
  COALESCE(last72_specimen, next24_specimen) AS specimen,
  COALESCE(last72_positiveculture, next24_positiveculture) AS positive_culture
FROM ab_tbl a
LEFT JOIN me_then_ab m2a
  ON a.subject_id = m2a.subject_id AND a.ab_id = m2a.ab_id AND m2a.micro_seq = 1
LEFT JOIN ab_then_me a2m
  ON a.subject_id = a2m.subject_id AND a.ab_id = a2m.ab_id AND a2m.micro_seq = 1
;
