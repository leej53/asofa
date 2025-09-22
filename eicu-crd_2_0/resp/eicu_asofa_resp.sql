-- =====================================================================
-- File: eicu_asofa_resp.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.eicu_asofa_resp
--
-- Purpose
--   Compute aSOFA-Resp and SOFA-Resp for the FIRST ICU stay (adults), using
--   only the features from eicu_resp_support_map. Output headers are aligned
--   to MIMIC for easy downstream merging.
--
-- Inputs
--   <PROJECT>.<DATASET>.eicu_resp_support_map
--
-- Output columns
--   patientunitstayid | asofa_resp_score | sofa_resp_score | length_of_stay | mortality_30day
--
-- Scoring
--   aSOFA-Resp:
--     * vent=1 & (ecmo OR pulm_vaso OR nmb OR prone) → 4
--     * vent=1 & no adjuncts → 3
--     * vent=0 & (niv OR highflow) → 2
--     * vent=0 & roomair=0 → 1
--     * vent=0 & roomair=1 → 0
--   SOFA-Resp (PF worst in 24h):
--     PF≥400→0, ≥300→1, ≥200→2, ≥100 & vent=1→3, <100 & vent=1→4; if vent=0 & PF<200 → 2.
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_asofa_resp AS
WITH s AS (
  SELECT * FROM `<PROJECT>.<DATASET>`.eicu_resp_support_map
),
scored AS (
  SELECT
    s.patientunitstayid,
    CASE
      WHEN IFNULL(s.vent, 0) = 1 AND (IFNULL(s.ecmo,0)=1 OR IFNULL(s.pulm_vaso,0)=1 OR IFNULL(s.nmb,0)=1 OR IFNULL(s.prone,0)=1) THEN 4
      WHEN IFNULL(s.vent, 0) = 1 THEN 3
      WHEN IFNULL(s.vent, 0) = 0 AND (IFNULL(s.niv,0)=1 OR IFNULL(s.highflow,0)=1) THEN 2
      WHEN IFNULL(s.vent, 0) = 0 AND IFNULL(s.roomair,0) = 0 THEN 1
      WHEN IFNULL(s.vent, 0) = 0 AND IFNULL(s.roomair,0) = 1 THEN 0
      ELSE 0
    END AS asofa_resp_score,
    CASE
      WHEN s.PF_ratio IS NULL THEN 0
      WHEN s.PF_ratio >= 400 THEN 0
      WHEN s.PF_ratio >= 300 THEN 1
      WHEN s.PF_ratio >= 200 THEN 2
      WHEN s.PF_ratio >= 100 AND IFNULL(s.vent,0) = 1 THEN 3
      WHEN s.PF_ratio < 100  AND IFNULL(s.vent,0) = 1 THEN 4
      ELSE 2  -- vent=0 and PF<200
    END AS sofa_resp_score,
    s.length_of_stay,
    s.mortality_30day
  FROM s
)
SELECT
  patientunitstayid,
  asofa_resp_score,
  sofa_resp_score,
  length_of_stay,
  mortality_30day
FROM scored
ORDER BY patientunitstayid;
