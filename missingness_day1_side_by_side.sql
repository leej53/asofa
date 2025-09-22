-- =====================================================================
-- File: day1_missingness_side_by_side.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Datasets:  <MIMIC_DATASET> (your MIMIC outputs/views), <EICU_DATASET> (your eICU outputs/views)
-- Table out: <PROJECT>.<OUTPUT_DATASET>.missingness_day1_side_by_side
--
-- Purpose
--   Produce a single, side-by-side summary of FIRST-24H missingness/edge cases
--   required by TRIPOD transparency, aligned with your final aSOFA pipeline.
--
-- Sources mirrored from your final repo & PhysioNet:
--   MIMIC-IV v3.1
--     - ICU stays/adults:       physionet-data.mimiciv_3_1_icu.icustays,
--                               physionet-data.mimiciv_3_1_hosp.patients
--     - MAP:                    physionet-data.mimiciv_3_1_icu.chartevents (220045, 223761)
--     - Vaso infusion events:   physionet-data.mimiciv_3_1_icu.inputevents
--     - Vent support timeline:  <PROJECT>.<MIMIC_DATASET>.ventilation   -- (starttime/endtime/ventilation_status)
--     - PF ratio (ABG):         physionet-data.mimiciv_3_1_derived.bg         -- (note: key is subject_id)
--     - Urine output:           physionet-data.mimiciv_3_1_icu.outputevents
--     - 1-day labs (Cr):        physionet-data.mimiciv_3_1_derived.first_day_lab
--
--   eICU-CRD
--     - ICU detail/adults:      physionet-data.eicu_crd_derived.icustay_detail
--     - Vaso orders & rates:    physionet-data.eicu_crd.infusiondrug (+ patient/admission weight)
--     - MAP (pivoted vitals):   physionet-data.eicu_crd_derived.pivoted_vital
--     - Resp support timeline:  physionet-data.eicu_crd_derived.debug_vent_tags  -- (has chartoffset)
--     - PF ratio (ABG):         physionet-data.eicu_crd_derived.pivoted_bg
--     - Urine output:           physionet-data.eicu_crd.intakeoutput
--     - 1-day labs (Cr):        physionet-data.eicu_crd_derived.labsfirstday
--
-- Notes
--   * “FIRST 24h” = [ICU intime, intime + 24h) for MIMIC (DATETIME window),
--                   [0, 1440] minutes offset for eICU (offset window).
--   * “Vaso order present but NO infusion rate” means: ≥1 order exists
--     but there is no non-NULL/non-zero infusion rate record in the window.
--   * “First-pressor decreased after 2nd/3rd added” is computed at the
--     **first addition event** (1→≥2 agents). We compare the **last recorded
--     rate** of the first agent just before addition vs. the **minimum rate**
--     of the same agent afterwards within the 24h window. Any decrease counts.
--   * PF ratio missing uses ABG rows requiring PaO2 & FiO2 (FiO2>0).
--   * “ABG after support started” counts if the earliest qualifying ABG occurs
--     at/after the earliest non-room-air respiratory support start.
--   * Urine output “missing” = no output row recorded in the first 24h window.
--   * This script does not alter your scoring tables; it only **reports** missingness.
--   * Output column names `mimic_total` / `eicu_total` are used to avoid duplicate-name errors.
--   * Placeholders: use <MIMIC_DATASET> for MIMIC-side derived tables, <EICU_DATASET> for eICU-side,
--     and <OUTPUT_DATASET> for the destination dataset.
-- =====================================================================
 
-- -----------------------------
-- 0) Parameters (replace via search/replace before run)
-- -----------------------------
-- DECLARE PROJECT STRING       DEFAULT "<PROJECT>";
-- DECLARE MIMIC_DS STRING      DEFAULT "<MIMIC_DATASET>";
-- DECLARE EICU_DS  STRING      DEFAULT "<EICU_DATASET>";
-- DECLARE OUTPUT_DS STRING     DEFAULT "<OUTPUT_DATASET>";
 
CREATE OR REPLACE TABLE `<PROJECT>.<OUTPUT_DATASET>`.missingness_day1_side_by_side AS
WITH 
-- =====================================================================
-- A) MIMIC-IV: Adult first ICU 24h window
-- =====================================================================
mimic_first_icu AS (
  SELECT
    i.subject_id, i.stay_id, i.hadm_id, i.intime, i.outtime,
    ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime) AS rn
  FROM `physionet-data.mimiciv_3_1_icu.icustays` i
  JOIN `physionet-data.mimiciv_3_1_hosp.patients` p
    ON p.subject_id = i.subject_id
  WHERE p.anchor_age >= 18
),
mimic_first_stay AS (
  SELECT subject_id, stay_id, hadm_id, intime, outtime,
         DATETIME_ADD(intime, INTERVAL 24 HOUR) AS intime_24h
  FROM mimic_first_icu
  WHERE rn = 1
),
mimic_N AS (SELECT COUNT(*) AS N FROM mimic_first_stay),
 
-- MAP (min over 24h)
mimic_min_map AS (
  SELECT
    fs.subject_id, fs.stay_id,
    MIN(CAST(c.valuenum AS FLOAT64)) AS min_map
  FROM mimic_first_stay fs
  JOIN `physionet-data.mimiciv_3_1_icu.chartevents` c
    ON c.stay_id = fs.stay_id
   AND c.charttime >= fs.intime AND c.charttime < fs.intime_24h
   AND c.valuenum IS NOT NULL
   AND c.itemid IN (220045, 223761)  -- invasive/noninvasive MAP
  GROUP BY fs.subject_id, fs.stay_id
),
mimic_map_missing AS (
  SELECT COUNTIF(mm.min_map IS NULL) AS n_missing
  FROM mimic_first_stay fs
  LEFT JOIN mimic_min_map mm
    ON mm.subject_id = fs.subject_id AND mm.stay_id = fs.stay_id
),
 
-- Vaso orders within 24h (any of SOFA agents)
mimic_vaso_orders AS (
  SELECT DISTINCT fs.subject_id, fs.stay_id
  FROM mimic_first_stay fs
  JOIN `physionet-data.mimiciv_3_1_icu.inputevents` ie
    ON ie.stay_id = fs.stay_id
   AND ie.starttime >= fs.intime AND ie.starttime < fs.intime_24h
   AND ie.itemid IN (221906,221289,221749,221986,221662,221653)  -- NE,Epi,Pheny,Vaso,Dopa,Dobu
),
mimic_vaso_rate_present AS (
  SELECT DISTINCT fs.subject_id, fs.stay_id
  FROM mimic_first_stay fs
  JOIN `physionet-data.mimiciv_3_1_icu.inputevents` ie
    ON ie.stay_id = fs.stay_id
   AND ie.starttime >= fs.intime AND ie.starttime < fs.intime_24h
   AND ie.itemid IN (221906,221289,221749,221986,221662,221653)
   AND ie.rate IS NOT NULL AND SAFE_CAST(ie.rate AS FLOAT64) != 0
),
mimic_vaso_order_but_no_rate AS (
  SELECT COUNT(*) AS n_missing
  FROM mimic_vaso_orders o
  LEFT JOIN mimic_vaso_rate_present r
    ON r.subject_id = o.subject_id AND r.stay_id = o.stay_id
  WHERE r.subject_id IS NULL
),
 
-- First-addition event (1→≥2 agents) and “first agent rate decreased”
mimic_vaso_timeline AS (
  SELECT
    fs.subject_id, fs.stay_id,
    CASE
      WHEN ie.itemid=221906 THEN 'norepinephrine'
      WHEN ie.itemid=221289 THEN 'epinephrine'
      WHEN ie.itemid=221749 THEN 'phenylephrine'
      WHEN ie.itemid=221986 THEN 'vasopressin'
      WHEN ie.itemid=221662 THEN 'dopamine'
      WHEN ie.itemid=221653 THEN 'dobutamine'
      ELSE NULL
    END AS agent,
    ie.starttime,
    SAFE_CAST(ie.rate AS FLOAT64) AS rate
  FROM mimic_first_stay fs
  JOIN `physionet-data.mimiciv_3_1_icu.inputevents` ie
    ON ie.stay_id = fs.stay_id
   AND ie.starttime >= fs.intime AND ie.starttime < fs.intime_24h
  WHERE ie.rate IS NOT NULL
    AND ie.itemid IN (221906,221289,221749,221986,221662,221653)
),
mimic_first_and_add AS (
  WITH per_agent AS (
    SELECT stay_id, agent, MIN(starttime) AS agent_start
    FROM mimic_vaso_timeline
    GROUP BY stay_id, agent
  ),
  first_sel AS (
    SELECT
      stay_id,
      agent AS first_agent,
      agent_start AS first_start
    FROM per_agent
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY agent_start, agent) = 1
  ),
  add_sel AS (
    SELECT
      pa.stay_id,
      MIN(pa.agent_start) AS t_add
    FROM per_agent pa
    JOIN first_sel f USING (stay_id)
    WHERE pa.agent_start > f.first_start
    GROUP BY pa.stay_id
  )
  SELECT
    f.stay_id,
    STRUCT(f.first_agent AS agent, f.first_start AS starttime) AS first_agent,
    a.t_add
  FROM first_sel f
  LEFT JOIN add_sel a USING (stay_id)
),
-- De-correlated computation of rate_before (last before t_add) and rate_after_min (min after t_add)
mimic_first_agent_info AS (
  SELECT stay_id, first_agent.agent AS agent, t_add
  FROM mimic_first_and_add
  WHERE t_add IS NOT NULL
),
mimic_rate_before AS (
  SELECT
    t.stay_id,
    t.rate AS rate_before
  FROM mimic_vaso_timeline t
  JOIN mimic_first_agent_info fa USING (stay_id)
  WHERE t.agent = fa.agent
    AND t.starttime < fa.t_add
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.stay_id ORDER BY t.starttime DESC) = 1
),
mimic_rate_after AS (
  SELECT
    t.stay_id,
    MIN(t.rate) AS rate_after_min
  FROM mimic_vaso_timeline t
  JOIN mimic_first_agent_info fa USING (stay_id)
  WHERE t.agent = fa.agent
    AND t.starttime >= fa.t_add
  GROUP BY t.stay_id
),
mimic_first_decrease AS (
  SELECT
    fa.stay_id,
    rb.rate_before,
    ra.rate_after_min
  FROM mimic_first_agent_info fa
  LEFT JOIN mimic_rate_before rb USING (stay_id)
  LEFT JOIN mimic_rate_after  ra USING (stay_id)
),
mimic_first_add_decrease_count AS (
  SELECT COUNTIF(rate_before IS NOT NULL AND rate_after_min IS NOT NULL AND rate_after_min < rate_before) AS n_hits
  FROM mimic_first_decrease
),
 
-- PF ratio missing (ABG within 24h)
mimic_pf AS (
  SELECT fs.stay_id,
         MIN(b.charttime) AS first_abg_time
  FROM mimic_first_stay fs
  JOIN `physionet-data.mimiciv_3_1_derived.bg` b
    ON b.subject_id = fs.subject_id           -- key is subject_id
   AND b.charttime >= fs.intime AND b.charttime < fs.intime_24h
   AND ((b.pao2fio2ratio IS NOT NULL AND b.pao2fio2ratio > 0)
        OR (b.po2 IS NOT NULL AND b.fio2 IS NOT NULL AND b.fio2 > 0))
  GROUP BY fs.stay_id
),
mimic_pf_missing AS (
  SELECT COUNTIF(p.first_abg_time IS NULL) AS n_missing
  FROM mimic_first_stay fs
  LEFT JOIN mimic_pf p USING (stay_id)
),
 
-- ABG after support start (uses <PROJECT>.<MIMIC_DATASET>.ventilation)
mimic_support_start AS (
  SELECT v.stay_id, MIN(v.starttime) AS support_start
  FROM `<PROJECT>.<MIMIC_DATASET>`.ventilation v
  JOIN mimic_first_stay fs ON fs.stay_id = v.stay_id
  WHERE v.starttime <= fs.intime_24h
    AND (v.endtime IS NULL OR v.endtime >= fs.intime)
    AND v.ventilation_status != 'NONE'
  GROUP BY v.stay_id
),
mimic_abg_after_support AS (
  SELECT COUNTIF(p.first_abg_time IS NOT NULL AND s.support_start IS NOT NULL AND p.first_abg_time >= s.support_start) AS n_hits
  FROM mimic_first_stay fs
  LEFT JOIN mimic_pf p USING (stay_id)
  LEFT JOIN mimic_support_start s USING (stay_id)
),
 
-- Urine output presence
mimic_uo_present AS (
  SELECT DISTINCT fs.stay_id
  FROM mimic_first_stay fs
  JOIN `physionet-data.mimiciv_3_1_icu.outputevents` o
    ON o.stay_id = fs.stay_id
   AND o.charttime >= fs.intime AND o.charttime < fs.intime_24h
   AND o.value IS NOT NULL
),
mimic_uo_missing AS (
  SELECT COUNTIF(p.stay_id IS NULL) AS n_missing
  FROM mimic_first_stay fs
  LEFT JOIN mimic_uo_present p USING (stay_id)
),
 
-- Creatinine missing (first day derived labs)
mimic_cr_missing AS (
  SELECT COUNTIF(l.creatinine_min IS NULL AND l.creatinine_max IS NULL) AS n_missing
  FROM mimic_first_stay fs
  LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_lab` l
    ON l.stay_id = fs.stay_id
)
 
-- =====================================================================
-- B) eICU-CRD: Adult first ICU 24h (offset window)
-- =====================================================================
,
eicu_first_stay AS (
  SELECT
    icu.patientunitstayid,
    icu.unitvisitnumber,
    CASE WHEN icu.age = '> 89' THEN 90 ELSE SAFE_CAST(icu.age AS INT64) END AS age
  FROM `physionet-data.eicu_crd_derived.icustay_detail` icu
  WHERE icu.age IS NOT NULL
    AND icu.gender IS NOT NULL
    AND icu.unitvisitnumber = 1
    AND (icu.age = '> 89' OR SAFE_CAST(icu.age AS INT64) >= 18)
),
eicu_N AS (SELECT COUNT(*) AS N FROM eicu_first_stay),
 
-- MAP from pivoted_vital
eicu_min_map AS (
  SELECT v.patientunitstayid,
         CASE
           WHEN MIN(v.ibp_mean)  IS NOT NULL THEN MIN(v.ibp_mean)
           WHEN MIN(v.nibp_mean) IS NOT NULL THEN MIN(v.nibp_mean)
           ELSE NULL
         END AS min_map
  FROM `physionet-data.eicu_crd_derived.pivoted_vital` v
  JOIN eicu_first_stay fs ON fs.patientunitstayid = v.patientunitstayid
  WHERE v.entryoffset BETWEEN 0 AND 1440
  GROUP BY v.patientunitstayid
),
eicu_map_missing AS (
  SELECT COUNTIF(mm.min_map IS NULL) AS n_missing
  FROM eicu_first_stay fs
  LEFT JOIN eicu_min_map mm
    ON mm.patientunitstayid = fs.patientunitstayid
),
 
-- Vaso orders & rates (drugrate normalized; prefer row weight, else admissionweight)
eicu_vaso_orders AS (
  SELECT DISTINCT i.patientunitstayid
  FROM `physionet-data.eicu_crd.infusiondrug` i
  JOIN eicu_first_stay fs ON fs.patientunitstayid = i.patientunitstayid
  WHERE i.infusionoffset BETWEEN 0 AND 1440
    AND (
      LOWER(i.drugname) LIKE '%norepinephrine%' OR
      LOWER(i.drugname) LIKE '%epinephrine%'   OR
      LOWER(i.drugname) LIKE '%dopamine%'      OR
      LOWER(i.drugname) LIKE '%dobutamine%'    OR
      LOWER(i.drugname) LIKE '%phenylephrine%' OR
      LOWER(i.drugname) LIKE '%vasopressin%'
    )
),
eicu_vaso_rate_present AS (
  SELECT DISTINCT i.patientunitstayid
  FROM `physionet-data.eicu_crd.infusiondrug` i
  LEFT JOIN `physionet-data.eicu_crd.patient` p USING (patientunitstayid)
  JOIN eicu_first_stay fs ON fs.patientunitstayid = i.patientunitstayid
  WHERE i.infusionoffset BETWEEN 0 AND 1440
    AND (
      LOWER(i.drugname) LIKE '%norepinephrine%' OR
      LOWER(i.drugname) LIKE '%epinephrine%'   OR
      LOWER(i.drugname) LIKE '%dopamine%'      OR
      LOWER(i.drugname) LIKE '%dobutamine%'    OR
      LOWER(i.drugname) LIKE '%phenylephrine%' OR
      LOWER(i.drugname) LIKE '%vasopressin%'
    )
    AND i.drugrate IS NOT NULL
    AND SAFE_CAST(i.drugrate AS FLOAT64) != 0
),
eicu_vaso_order_but_no_rate AS (
  SELECT COUNT(*) AS n_missing
  FROM eicu_vaso_orders o
  LEFT JOIN eicu_vaso_rate_present r
    ON r.patientunitstayid = o.patientunitstayid
  WHERE r.patientunitstayid IS NULL
),
 
-- First-addition event (1→≥2) & first-agent decrease
eicu_vaso_timeline AS (
  SELECT
    i.patientunitstayid,
    CASE
      WHEN LOWER(i.drugname) LIKE '%norepinephrine%' THEN 'norepinephrine'
      WHEN LOWER(i.drugname) LIKE '%epinephrine%'     THEN 'epinephrine'
      WHEN LOWER(i.drugname) LIKE '%dopamine%'        THEN 'dopamine'
      WHEN LOWER(i.drugname) LIKE '%dobutamine%'      THEN 'dobutamine'
      WHEN LOWER(i.drugname) LIKE '%phenylephrine%'   THEN 'phenylephrine'
      WHEN LOWER(i.drugname) LIKE '%vasopressin%'     THEN 'vasopressin'
      ELSE NULL
    END AS agent,
    i.infusionoffset AS tmin,
    -- rate normalized to mcg/kg/min (approx): prefer row patientweight, else admissionweight
    CASE
      WHEN LOWER(i.drugname) LIKE '%mcg/kg/min%' THEN SAFE_CAST(i.drugrate AS FLOAT64)
      ELSE SAFE_CAST(i.drugrate AS FLOAT64) / NULLIF(COALESCE(i.patientweight, p.admissionweight), 0)
    END AS rate_adj
  FROM `physionet-data.eicu_crd.infusiondrug` i
  LEFT JOIN `physionet-data.eicu_crd.patient` p USING (patientunitstayid)
  JOIN eicu_first_stay fs ON fs.patientunitstayid = i.patientunitstayid
  WHERE i.infusionoffset BETWEEN 0 AND 1440
    AND (
      LOWER(i.drugname) LIKE '%norepinephrine%' OR
      LOWER(i.drugname) LIKE '%epinephrine%'   OR
      LOWER(i.drugname) LIKE '%dopamine%'      OR
      LOWER(i.drugname) LIKE '%dobutamine%'    OR
      LOWER(i.drugname) LIKE '%phenylephrine%' OR
      LOWER(i.drugname) LIKE '%vasopressin%'
    )
    AND i.drugrate IS NOT NULL
),
eicu_first_and_add AS (
  WITH per_agent AS (
    SELECT patientunitstayid, agent, MIN(tmin) AS agent_start
    FROM eicu_vaso_timeline
    GROUP BY patientunitstayid, agent
  ),
  first_sel AS (
    SELECT
      patientunitstayid,
      agent AS first_agent,
      agent_start AS first_start
    FROM per_agent
    QUALIFY ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY agent_start, agent) = 1
  ),
  add_sel AS (
    SELECT
      pa.patientunitstayid,
      MIN(pa.agent_start) AS t_add
    FROM per_agent pa
    JOIN first_sel f USING (patientunitstayid)
    WHERE pa.agent_start > f.first_start
    GROUP BY pa.patientunitstayid
  )
  SELECT
    f.patientunitstayid,
    STRUCT(f.first_agent AS agent, f.first_start AS tmin) AS first_agent,
    a.t_add
  FROM first_sel f
  LEFT JOIN add_sel a USING (patientunitstayid)
),
-- De-correlated computation for eICU (before/after)
eicu_first_agent_info AS (
  SELECT patientunitstayid, first_agent.agent AS agent, t_add
  FROM eicu_first_and_add
  WHERE t_add IS NOT NULL
),
eicu_rate_before AS (
  SELECT
    t.patientunitstayid,
    t.rate_adj AS rate_before
  FROM eicu_vaso_timeline t
  JOIN eicu_first_agent_info fa USING (patientunitstayid)
  WHERE t.agent = fa.agent
    AND t.tmin < fa.t_add
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.patientunitstayid ORDER BY t.tmin DESC) = 1
),
eicu_rate_after AS (
  SELECT
    t.patientunitstayid,
    MIN(t.rate_adj) AS rate_after_min
  FROM eicu_vaso_timeline t
  JOIN eicu_first_agent_info fa USING (patientunitstayid)
  WHERE t.agent = fa.agent
    AND t.tmin >= fa.t_add
  GROUP BY t.patientunitstayid
),
eicu_first_decrease AS (
  SELECT
    fa.patientunitstayid,
    rb.rate_before,
    ra.rate_after_min
  FROM eicu_first_agent_info fa
  LEFT JOIN eicu_rate_before rb USING (patientunitstayid)
  LEFT JOIN eicu_rate_after  ra USING (patientunitstayid)
),
eicu_first_add_decrease_count AS (
  SELECT COUNTIF(rate_before IS NOT NULL AND rate_after_min IS NOT NULL AND rate_after_min < rate_before) AS n_hits
  FROM eicu_first_decrease
),
 
-- PF ratio missing (pivoted_bg)
eicu_pf AS (
  SELECT b.patientunitstayid,
         MIN(b.chartoffset) AS first_abg_offset
  FROM `physionet-data.eicu_crd_derived.pivoted_bg` b
  JOIN eicu_first_stay fs ON fs.patientunitstayid = b.patientunitstayid
  WHERE b.chartoffset BETWEEN 0 AND 1440
    AND b.pao2 IS NOT NULL AND b.fio2 IS NOT NULL AND b.fio2 > 0
  GROUP BY b.patientunitstayid
),
eicu_pf_missing AS (
  SELECT COUNTIF(p.first_abg_offset IS NULL) AS n_missing
  FROM eicu_first_stay fs
  LEFT JOIN eicu_pf p USING (patientunitstayid)
),
 
-- Earliest respiratory support start (non-room-air) in 24h; uses DEBUG_VENT_TAGS (has chartoffset)
eicu_support_start AS (
  SELECT
    v.patientunitstayid,
    MIN(v.chartoffset) AS support_start
  FROM `physionet-data.eicu_crd_derived.debug_vent_tags` v
  JOIN eicu_first_stay fs ON fs.patientunitstayid = v.patientunitstayid
  WHERE v.chartoffset BETWEEN 0 AND 1440
    AND (
      v.aerosolmask=1 OR v.assistcontrol=1 OR v.bagandmaskppv=1 OR v.bilevel=1 OR v.bipap=1 OR v.bivent=1 OR
      v.cpap=1 OR v.ecmo=1 OR v.ett=1 OR v.extubated=1 OR v.facemask=1 OR v.faceshield=1 OR v.facetent=1 OR
      v.highflow=1 OR v.nasalcannula=1 OR v.nasalprongs=1 OR v.nebulizer=1 OR v.nippv=1 OR v.niv=1 OR
      v.nonrebreather=1 OR v.pressuresupport=1 OR v.prvc=1 OR v.simv=1 OR v.speakingvalve=1 OR v.timecontrol=1 OR
      v.tpiece=1 OR v.trach=1 OR v.trachcollar=1 OR v.trachmask=1 OR v.trachshield=1 OR v.tto=1 OR
      v.ventilator=1 OR v.ventimask=1 OR v.volumecontrol=1
    )
  GROUP BY v.patientunitstayid
),
eicu_abg_after_support AS (
  SELECT COUNTIF(p.first_abg_offset IS NOT NULL AND s.support_start IS NOT NULL AND p.first_abg_offset >= s.support_start) AS n_hits
  FROM eicu_first_stay fs
  LEFT JOIN eicu_pf p USING (patientunitstayid)
  LEFT JOIN eicu_support_start s USING (patientunitstayid)
),
 
-- Urine output presence
eicu_uo_present AS (
  SELECT DISTINCT fs.patientunitstayid
  FROM eicu_first_stay fs
  JOIN `physionet-data.eicu_crd.intakeoutput` io
    ON io.patientunitstayid = fs.patientunitstayid
   AND io.intakeoutputoffset BETWEEN 0 AND 1440
   AND io.outputtotal IS NOT NULL
),
eicu_uo_missing AS (
  SELECT COUNTIF(p.patientunitstayid IS NULL) AS n_missing
  FROM eicu_first_stay fs
  LEFT JOIN eicu_uo_present p USING (patientunitstayid)
),
 
-- Creatinine missing (labsfirstday)
eicu_cr_missing AS (
  SELECT COUNTIF(l.CREATININE_min IS NULL AND l.CREATININE_max IS NULL) AS n_missing
  FROM eicu_first_stay fs
  LEFT JOIN `physionet-data.eicu_crd_derived.labsfirstday` l
    ON l.patientunitstayid = fs.patientunitstayid
)
 
-- =====================================================================
-- C) Final side-by-side table
-- =====================================================================
SELECT
  metric_id,
  metric_label,
  mimic_n,
  (SELECT N FROM mimic_N) AS mimic_total,
  SAFE_DIVIDE(mimic_n, (SELECT N FROM mimic_N)) * 100 AS mimic_pct,
  eicu_n,
  (SELECT N FROM eicu_N) AS eicu_total,
  SAFE_DIVIDE(eicu_n,  (SELECT N FROM eicu_N))  * 100 AS eicu_pct
FROM (
  SELECT 1 AS metric_id, 'CV: MAP missing (min over 0–24h)' AS metric_label,
         (SELECT n_missing FROM mimic_map_missing) AS mimic_n,
         (SELECT n_missing FROM eicu_map_missing)  AS eicu_n
  UNION ALL
  SELECT 2, 'CV: Vaso order present but NO non-null infusion rate',
         (SELECT n_missing FROM mimic_vaso_order_but_no_rate),
         (SELECT n_missing FROM eicu_vaso_order_but_no_rate)
  UNION ALL
  SELECT 3, 'CV: At first addition of 2nd/3rd vaso, 1st-agent rate decreased',
         (SELECT n_hits FROM mimic_first_add_decrease_count),
         (SELECT n_hits FROM eicu_first_add_decrease_count)
  UNION ALL
  SELECT 4, 'Resp: PF ratio missing (ABG with PaO2 & FiO2 in 0–24h)',
         (SELECT n_missing FROM mimic_pf_missing),
         (SELECT n_missing FROM eicu_pf_missing)
  UNION ALL
  SELECT 5, 'Resp: First ABG at/after respiratory support start (0–24h)',
         (SELECT n_hits FROM mimic_abg_after_support),
         (SELECT n_hits FROM eicu_abg_after_support)
  UNION ALL
  SELECT 6, 'Renal: Creatinine missing (first-day min & max both NULL)',
         (SELECT n_missing FROM mimic_cr_missing),
         (SELECT n_missing FROM eicu_cr_missing)
  UNION ALL
  SELECT 7, 'Renal: Urine output missing (no output events in 0–24h)',
         (SELECT n_missing FROM mimic_uo_missing),
         (SELECT n_missing FROM eicu_uo_missing)
)
ORDER BY metric_id;