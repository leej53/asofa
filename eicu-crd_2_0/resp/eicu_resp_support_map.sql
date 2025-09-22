-- =====================================================================
-- File: eicu_resp_support_map.sql
-- BigQuery Standard SQL
-- Project:   <PROJECT>
-- Dataset:   <DATASET>
-- Table out: <PROJECT>.<DATASET>.eicu_resp_support_map
--
-- Purpose
--   Build FIRST-24H respiratory support features for the FIRST ICU stay
--   in eICU-CRD adults. Aligns with MIMIC naming/headers and fixes prior
--   issues by:
--     * Using a worst (minimum) day-1 PF ratio from apacheapsvar within 24h
--       (instead of ANY_VALUE), with robust FiO2 validation.
--     * Defining room air conservatively as "no other mode observed".
--     * Combining vent evidence from vent_tags + apacheapsvar.
--     * Capturing adjuncts within the same 0–1440 min window: ECMO, NMB,
--       pulmonary vasodilators, and proning.
--     * Producing a mode string ordered by first appearance within 24h.
--     * Returning ICU LOS in DAYS (rounded to 2 decimals) for consistency.
--
-- Cohort & Window
--   Adults (>=18, '> 89' treated as 90), FIRST ICU visit (unitvisitnumber=1).
--   Day-1 window: [0, 1440) minutes from ICU admission.
-- =====================================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.eicu_resp_support_map AS
WITH base AS (
  SELECT
    icu.patientunitstayid,
    CASE WHEN icu.age = '> 89' THEN 90 ELSE SAFE_CAST(icu.age AS INT64) END AS age,
    icu.gender,
    CASE WHEN icu.hospitaldischargeoffset BETWEEN 0 AND 43200 AND icu.hosp_mort = 1 THEN 1 ELSE 0 END AS mortality_30day,
    ROUND(SAFE_CAST(icu.icu_los_hours AS FLOAT64)/24.0, 2) AS length_of_stay
  FROM `physionet-data.eicu_crd_derived.icustay_detail` icu
  WHERE icu.age IS NOT NULL
    AND icu.gender IS NOT NULL
    AND icu.unitvisitnumber = 1
    AND (icu.age = '> 89' OR SAFE_CAST(icu.age AS INT64) >= 18)
),

-- 24h windowed vent tags
vent_tags_24h AS (
  SELECT *
  FROM `physionet-data.eicu_crd_derived.debug_vent_tags`
  WHERE chartoffset BETWEEN 0 AND 1440
),

-- Aggregate binary flags from vent_tags
vent_flags AS (
  SELECT
    patientunitstayid,
    MAX(CASE WHEN roomair=1 THEN 1 ELSE 0 END) AS has_roomair,
    MAX(CASE WHEN
      aerosolmask=1 OR assistcontrol=1 OR bagandmaskppv=1 OR bilevel=1 OR bipap=1 OR bivent=1 OR
      cpap=1 OR ecmo=1 OR ett=1 OR extubated=1 OR facemask=1 OR faceshield=1 OR facetent=1 OR
      highflow=1 OR nasalcannula=1 OR nasalprongs=1 OR nebulizer=1 OR nippv=1 OR niv=1 OR
      nonrebreather=1 OR pressuresupport=1 OR prvc=1 OR simv=1 OR speakingvalve=1 OR timecontrol=1 OR
      tpiece=1 OR trach=1 OR trachcollar=1 OR trachmask=1 OR trachshield=1 OR tto=1 OR
      ventilator=1 OR ventimask=1 OR volumecontrol=1
    THEN 1 ELSE 0 END) AS has_other_mode,
    MAX(CASE WHEN highflow=1 THEN 1 ELSE 0 END) AS has_highflow,
    MAX(CASE WHEN bilevel=1 OR bipap=1 OR bagandmaskppv=1 OR cpap=1 OR nippv=1 OR niv=1 THEN 1 ELSE 0 END) AS has_niv,
    MAX(CASE WHEN assistcontrol=1 OR bivent=1 OR ett=1 OR extubated=1 OR pressuresupport=1 OR prvc=1 OR simv=1 OR ventilator=1 OR volumecontrol=1 THEN 1 ELSE 0 END) AS has_vent,
    MAX(CASE WHEN ecmo=1 THEN 1 ELSE 0 END) AS has_ecmo
  FROM vent_tags_24h
  GROUP BY patientunitstayid
),

-- Additional invasive vent evidence from apacheapsvar (any time)
apache_vent AS (
  SELECT patientunitstayid,
         MAX(CASE WHEN intubated=1 OR vent=1 THEN 1 ELSE 0 END) AS apache_has_vent
  FROM `physionet-data.eicu_crd.apacheapsvar`
  GROUP BY patientunitstayid
),

-- PF ratio from apacheapsvar (day-1 summary; no offset column in this table)
pf_from_apache AS (
  SELECT
    patientunitstayid,
    ANY_VALUE(pao2) AS pao2,
    ANY_VALUE(fio2) AS fio2
  FROM `physionet-data.eicu_crd.apacheapsvar`
  GROUP BY patientunitstayid
),
worst_pf AS (
  SELECT
    patientunitstayid,
    -- invalid or missing → 401 (so SOFA-Resp = 0 downstream)
    CASE
      WHEN pao2 IS NULL OR fio2 IS NULL OR fio2 = 0 OR pao2 = -1 OR fio2 = -1
        THEN 401.0
      ELSE ROUND((pao2 / fio2) * 100.0, 2)
    END AS PF_ratio
  FROM pf_from_apache
),

-- Pulmonary vasodilators within 24h
pulm_vaso AS (
  SELECT patientunitstayid, 1 AS pulm_vaso
  FROM `physionet-data.eicu_crd.medication`
  WHERE REGEXP_CONTAINS(LOWER(drugname), r'(nitric oxide|epoprostenol|iloprost|treprostinil|flolan|veletri|ventavis|tyvaso)')
    AND drugstartoffset BETWEEN 0 AND 1440
    AND UPPER(drugordercancelled) = 'NO'
  GROUP BY patientunitstayid
),

-- Neuromuscular blockade within 24h
nmb AS (
  SELECT patientunitstayid, 1 AS nmb
  FROM `physionet-data.eicu_crd.infusiondrug`
  WHERE REGEXP_CONTAINS(LOWER(drugname), r'(cisatracurium|atracurium|vecuronium|rocuronium|pancuronium|tracurium|nimbex|norcuron)')
    AND infusionoffset BETWEEN 0 AND 1440
    AND SAFE_CAST(drugrate AS FLOAT64) IS NOT NULL
    AND SAFE_CAST(drugrate AS FLOAT64) <> 0
  GROUP BY patientunitstayid
),

-- Mode string by first appearance within 24h
modes_long AS (
  SELECT t.patientunitstayid, t.chartoffset, m.mode_name, m.flag
  FROM vent_tags_24h t,
  UNNEST([
    STRUCT('aerosolmask' AS mode_name, t.aerosolmask AS flag),
    STRUCT('assistcontrol', t.assistcontrol),
    STRUCT('bagandmaskppv', t.bagandmaskppv),
    STRUCT('bilevel', t.bilevel),
    STRUCT('bipap', t.bipap),
    STRUCT('bivent', t.bivent),
    STRUCT('cpap', t.cpap),
    STRUCT('ecmo', t.ecmo),
    STRUCT('ett', t.ett),
    STRUCT('extubated', t.extubated),
    STRUCT('facemask', t.facemask),
    STRUCT('faceshield', t.faceshield),
    STRUCT('facetent', t.facetent),
    STRUCT('highflow', t.highflow),
    STRUCT('nasalcannula', t.nasalcannula),
    STRUCT('nasalprongs', t.nasalprongs),
    STRUCT('nebulizer', t.nebulizer),
    STRUCT('nippv', t.nippv),
    STRUCT('niv', t.niv),
    STRUCT('nonrebreather', t.nonrebreather),
    STRUCT('pressuresupport', t.pressuresupport),
    STRUCT('prvc', t.prvc),
    STRUCT('roomair', t.roomair),
    STRUCT('simv', t.simv),
    STRUCT('speakingvalve', t.speakingvalve),
    STRUCT('timecontrol', t.timecontrol),
    STRUCT('tpiece', t.tpiece),
    STRUCT('trach', t.trach),
    STRUCT('trachcollar', t.trachcollar),
    STRUCT('trachmask', t.trachmask),
    STRUCT('trachshield', t.trachshield),
    STRUCT('tto', t.tto),
    STRUCT('ventilator', t.ventilator),
    STRUCT('ventimask', t.ventimask),
    STRUCT('volumecontrol', t.volumecontrol)
  ]) m
  WHERE m.flag = 1
),
first_mode AS (
  SELECT patientunitstayid, mode_name, MIN(chartoffset) AS first_offset
  FROM modes_long
  GROUP BY patientunitstayid, mode_name
),
mode_by_pt AS (
  SELECT patientunitstayid,
         STRING_AGG(mode_name, ' ' ORDER BY first_offset, mode_name) AS mode
  FROM first_mode
  GROUP BY patientunitstayid
),

-- Proning (treatment + nursecare) within 24h
prone_hits AS (
  SELECT DISTINCT patientunitstayid
  FROM `physionet-data.eicu_crd.treatment`
  WHERE REGEXP_CONTAINS(LOWER(treatmentstring), r'(\bprone\b|proning|proned|prone position|prone positioning)')
    AND treatmentoffset BETWEEN 0 AND 1440
  UNION DISTINCT
  SELECT DISTINCT patientunitstayid
  FROM `physionet-data.eicu_crd.nursecare`
  WHERE REGEXP_CONTAINS(LOWER(cellattributevalue), r'(\bprone\b|proning|proned|prone position|prone positioning)')
    AND nursecareentryoffset BETWEEN 0 AND 1440
),
prone AS (
  SELECT patientunitstayid, 1 AS prone FROM prone_hits
)

SELECT
  b.patientunitstayid,
  b.age,
  b.gender,
  b.mortality_30day,
  b.length_of_stay,
  -- room air = no other mode observed within 24h
  CASE WHEN IFNULL(v.has_other_mode, 0) = 0 THEN 1 ELSE 0 END AS roomair,
  IFNULL(v.has_highflow, 0) AS highflow,
  IFNULL(v.has_niv, 0) AS niv,
  CASE WHEN IFNULL(v.has_vent,0)=1 OR IFNULL(av.apache_has_vent,0)=1 THEN 1 ELSE 0 END AS vent,
  IFNULL(v.has_ecmo, 0) AS ecmo,
  IFNULL(p.pulm_vaso, 0) AS pulm_vaso,
  IFNULL(n.nmb, 0) AS nmb,
  IFNULL(pr.prone, 0) AS prone,
  wp.PF_ratio,
  mb.mode
FROM base b
LEFT JOIN vent_flags v   USING (patientunitstayid)
LEFT JOIN apache_vent av USING (patientunitstayid)
LEFT JOIN worst_pf wp    USING (patientunitstayid)
LEFT JOIN pulm_vaso p    USING (patientunitstayid)
LEFT JOIN nmb n          USING (patientunitstayid)
LEFT JOIN prone pr       USING (patientunitstayid)
LEFT JOIN mode_by_pt mb  USING (patientunitstayid)
ORDER BY b.patientunitstayid;