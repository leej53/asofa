-- BigQuery Standard SQL
-- Output: <PROJECT>.<DATASET>.ventilator_setting
--
-- Purpose
--   Create a pivoted table of ventilator settings/oxygen delivery per charttime,
--   restricted to ADULTS (age >= 18). This is a staging table for building
--   device/mode-based ventilation states.
--
-- Notes
--   * We filter to adults via patients.anchor_age >= 18.
--   * We do NOT restrict to first ICU stay here (keep full device signals);
--     later scripts will connect to the first ICU window.
--   * Value cleaning mirrors MIMIC-LCP logic (FiO2 normalization, etc.).

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.ventilator_setting AS
WITH ce AS (
  SELECT
    ce.subject_id,
    ce.stay_id,
    ce.charttime,
    ce.itemid,
    ce.value,
    CASE
      WHEN ce.itemid = 223835 THEN
        CASE
          WHEN ce.valuenum BETWEEN 0.20 AND 1 THEN ce.valuenum * 100
          WHEN ce.valuenum > 1 AND ce.valuenum < 20 THEN NULL
          WHEN ce.valuenum BETWEEN 20 AND 100 THEN ce.valuenum
          ELSE NULL
        END
      WHEN ce.itemid IN (220339, 224700) THEN
        CASE WHEN ce.valuenum > 100 OR ce.valuenum < 0 THEN NULL ELSE ce.valuenum END
      ELSE ce.valuenum
    END AS valuenum,
    ce.valueuom,
    ce.storetime
  FROM `physionet-data.mimiciv_3_1_icu.chartevents` AS ce
  JOIN `physionet-data.mimiciv_3_1_hosp.patients` p
    ON p.subject_id = ce.subject_id
  WHERE
    p.anchor_age >= 18
    AND ce.value IS NOT NULL
    AND ce.stay_id IS NOT NULL
    AND ce.itemid IN (
      224688, 224689, 224690,           -- respiratory rates
      224687, 224685, 224684, 224686,   -- minute volume / tidal volumes
      224696,                           -- plateau pressure
      220339, 224700,                   -- PEEP
      223835,                           -- FiO2
      224691,                           -- flow rate
      223849, 229314,                   -- ventilator modes (GE, Hamilton)
      223848                            -- ventilator type
    )
)
SELECT
  subject_id,
  MAX(stay_id) AS stay_id,
  charttime,
  MAX(CASE WHEN itemid = 224688 THEN valuenum END) AS respiratory_rate_set,
  MAX(CASE WHEN itemid = 224690 THEN valuenum END) AS respiratory_rate_total,
  MAX(CASE WHEN itemid = 224689 THEN valuenum END) AS respiratory_rate_spontaneous,
  MAX(CASE WHEN itemid = 224687 THEN valuenum END) AS minute_volume,
  MAX(CASE WHEN itemid = 224684 THEN valuenum END) AS tidal_volume_set,
  MAX(CASE WHEN itemid = 224685 THEN valuenum END) AS tidal_volume_observed,
  MAX(CASE WHEN itemid = 224686 THEN valuenum END) AS tidal_volume_spontaneous,
  MAX(CASE WHEN itemid = 224696 THEN valuenum END) AS plateau_pressure,
  MAX(CASE WHEN itemid IN (220339, 224700) THEN valuenum END) AS peep,
  MAX(CASE WHEN itemid = 223835 THEN valuenum END) AS fio2,
  MAX(CASE WHEN itemid = 224691 THEN valuenum END) AS flow_rate,
  MAX(CASE WHEN itemid = 223849 THEN value   END) AS ventilator_mode,
  MAX(CASE WHEN itemid = 229314 THEN value   END) AS ventilator_mode_hamilton,
  MAX(CASE WHEN itemid = 223848 THEN value   END) AS ventilator_type
FROM ce
GROUP BY subject_id, charttime;
