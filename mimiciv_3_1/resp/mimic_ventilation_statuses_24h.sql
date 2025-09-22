-- BigQuery Standard SQL
-- Output: <PROJECT>.<DATASET>.ventilation_statuses_24h
--
-- Purpose
--   Produce a SINGLE human-readable column, ventilation_statuses_24h,
--   listing (comma-separated) all ventilation states observed during
--   the FIRST 24 hours of the FIRST ICU stay (ADULTS only, age >= 18).
--   Pre-ICU ventilation that persists into the 24h window is included.
--
-- Window & Overlap
--   ICU window: [intime, intime + 24h)
--   Include any event with: event.start <= intime+24h  AND  (event.end IS NULL OR event.end >= intime)
--
-- Notes
--   * No ARRAY output (avoids "Array cannot have a null element" issues).
--   * STRING_AGG ignores NULL inputs by default; when no status exists,
--     we return an empty string via IFNULL(..., '').
--   * Status labels are normalized to UPPERCASE for downstream consistency.

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.ventilation_statuses_24h AS
WITH
-- (0) Adult-first ICU cohort
first_icu AS (
  SELECT
    i.subject_id,
    i.stay_id,
    i.intime,
    ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime) AS rn
  FROM `physionet-data.mimiciv_3_1_icu.icustays` AS i
  JOIN `physionet-data.mimiciv_3_1_hosp.patients` AS p
    ON p.subject_id = i.subject_id
  WHERE p.anchor_age >= 18
),
first_stay AS (
  SELECT
    subject_id,
    stay_id,
    intime,
    DATETIME_ADD(intime, INTERVAL 24 HOUR) AS intime_24h
  FROM first_icu
  WHERE rn = 1
),

-- (1) Ventilation events that overlap the 24h ICU window
--     Source events come from your ventilation table built upstream.
vent24h AS (
  SELECT
    v.stay_id,
    UPPER(v.ventilation_status) AS ventilation_status,
    v.starttime,
    v.endtime
  FROM `<PROJECT>.<DATASET>`.ventilation AS v
  JOIN first_stay fi
    ON v.stay_id = fi.stay_id
  WHERE
    v.starttime <= fi.intime_24h
    AND (v.endtime IS NULL OR v.endtime >= fi.intime)
),

-- (2) First appearance time per status within the 24h-overlap set
ranked_status AS (
  SELECT
    fi.subject_id,
    fi.stay_id,
    vent24h.ventilation_status,
    MIN(vent24h.starttime) AS first_start
  FROM first_stay fi
  LEFT JOIN vent24h
    ON fi.stay_id = vent24h.stay_id
  GROUP BY fi.subject_id, fi.stay_id, vent24h.ventilation_status
)

-- (3) Concatenate statuses in the order of first appearance
SELECT
  subject_id,
  stay_id,
  IFNULL(
    STRING_AGG(ventilation_status ORDER BY first_start),
    ''
  ) AS ventilation_statuses_24h
FROM ranked_status
GROUP BY subject_id, stay_id
ORDER BY subject_id, stay_id;
