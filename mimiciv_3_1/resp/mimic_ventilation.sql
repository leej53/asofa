-- BigQuery Standard SQL
-- Output: <PROJECT>.<DATASET>.ventilation
--
-- Purpose
--   Build time-range events of ventilation / oxygen device categories per stay,
--   using a **gapless** segmentation policy aligned with our day-1 aSOFA-Resp use case.
--   We classify each charttime into one of SIX categories and aggregate into
--   non-overlapping events. Adults only (age >= 18).
--
-- Why gapless?
--   The previous implementation split events when a documentation gap >= 14 hours
--   occurred. For our purpose (detecting whether a support modality was present at
--   least once in the first 24h), gap-based splitting can create false negatives
--   when charting is sparse. Here we remove the gap rule: events are split **only**
--   when the classified category changes. This makes downstream 24h-overlap logic
--   more robust and consistent with our eICU approach.
--
-- Categories (priority when conflicting signals exist at the same charttime):
--   INVASIVEVENT > TRACHEOSTOMY > NONINVASIVEVENT > HFNC > SUPPLEMENTALOXYGEN > NONE
--
-- Windowing
--   This table is global per-stay (not restricted to the first ICU 24h window).
--   Overlap with the ICU 24h window is handled by `mimic_ventilation_statuses_24h.sql`.
--
-- Inputs
--   <PROJECT>.<DATASET>.ventilator_setting   (pivoted device/mode signals; adults only)
--   physionet-data.mimiciv_3_1_derived.oxygen_delivery  (adults only via patients table)
--   physionet-data.mimiciv_3_1_hosp.patients            (to filter adults)
--
-- Output schema
--   stay_id | starttime | endtime | ventilation_status (UPPERCASE category)
--
-- Notes
--   * If multiple signals exist at the same charttime, we resolve with the priority above.
--   * Event endtime is set to the next charttime in the same sequence when available;
--     otherwise it equals the last charttime of the sequence. We do **not** extend
--     beyond observed timestamps (no artificial padding).
--   * Keep category labels stable; downstream code expects these exact strings.

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.ventilation AS
WITH tm AS (
  -- All charttimes from ventilator_setting (built upstream) and oxygen_delivery (MIT-LCP)
  SELECT vs.stay_id, vs.charttime
  FROM `<PROJECT>.<DATASET>`.ventilator_setting AS vs
  UNION ALL
  SELECT od.stay_id, od.charttime
  FROM `physionet-data.mimiciv_3_1_derived.oxygen_delivery` AS od
  JOIN `physionet-data.mimiciv_3_1_hosp.patients` AS p
    ON p.subject_id = od.subject_id
  WHERE p.anchor_age >= 18
),

-- Classify each charttime into a single category, resolving conflicts by priority
-- Extract raw signals at each charttime, plus harmonized mode fields
sig AS (
  SELECT
    tm.stay_id,
    tm.charttime,
    od.o2_delivery_device_1,
    COALESCE(vset.ventilator_mode, vset.ventilator_mode_hamilton) AS vent_mode,
    vset.ventilator_mode_hamilton AS vent_mode_hamilton
  FROM tm
  LEFT JOIN `<PROJECT>.<DATASET>`.ventilator_setting AS vset
    ON tm.stay_id = vset.stay_id AND tm.charttime = vset.charttime
  LEFT JOIN `physionet-data.mimiciv_3_1_derived.oxygen_delivery` AS od
    ON tm.stay_id = od.stay_id AND tm.charttime = od.charttime
),

-- Classify each charttime into a single category, resolving conflicts by priority
classify AS (
  SELECT
    s.stay_id,
    s.charttime,
    s.o2_delivery_device_1,
    s.vent_mode,
    s.vent_mode_hamilton,
    CASE
      -- Invasive ventilation evidence (ETT or specific ventilator modes)
      WHEN s.o2_delivery_device_1 IN ('Endotracheal tube')
        OR s.vent_mode IN (
          '(S) CMV','APRV','APRV/Biphasic+ApnPress','APRV/Biphasic+ApnVol',
          'APV (cmv)','Ambient','Apnea Ventilation','CMV','CMV/ASSIST',
          'CMV/ASSIST/AutoFlow','CMV/AutoFlow','CPAP/PPS','CPAP/PSV',
          'CPAP/PSV+Apn TCPL','CPAP/PSV+ApnPres','CPAP/PSV+ApnVol','MMV',
          'MMV/AutoFlow','MMV/PSV','MMV/PSV/AutoFlow','P-CMV','PCV+',
          'PCV+/PSV','PCV+Assist','PRES/AC','PRVC/AC','PRVC/SIMV','PSV/SBT',
          'SIMV','SIMV/AutoFlow','SIMV/PRES','SIMV/PSV','SIMV/PSV/AutoFlow',
          'SIMV/VOL','SYNCHRON MASTER','SYNCHRON SLAVE','VOL/AC'
        )
        OR s.vent_mode_hamilton IN ('APRV','APV (cmv)','(S) CMV','P-CMV','SIMV','APV (simv)','P-SIMV','VS','ASV')
      THEN 'INVASIVEVENT'

      -- Noninvasive ventilation
      WHEN s.o2_delivery_device_1 IN ('Bipap mask ','CPAP mask ')
        OR s.vent_mode_hamilton IN ('DuoPaP','NIV','NIV-ST')
      THEN 'NONINVASIVEVENT'

      -- High-flow nasal cannula
      WHEN s.o2_delivery_device_1 IN ('High flow nasal cannula')
      THEN 'HFNC'

      -- Tracheostomy devices
      WHEN s.o2_delivery_device_1 IN ('Tracheostomy tube','Trach mask ')
      THEN 'TRACHEOSTOMY'

      -- Supplemental oxygen (low-flow etc.)
      WHEN s.o2_delivery_device_1 IN (
        'Non-rebreather','Face tent','Aerosol-cool','Venti mask ','Medium conc mask ',
        'Ultrasonic neb','Vapomist','Oxymizer','High flow neb','Nasal cannula'
      )
      THEN 'SUPPLEMENTALOXYGEN'

      -- Explicit none
      WHEN s.o2_delivery_device_1 IN ('None')
      THEN 'NONE'

      ELSE NULL
    END AS ventilation_status
  FROM sig s
),

-- Prepare for event segmentation (gapless): mark a new event only when the category changes
mark AS (
  SELECT
    stay_id,
    charttime,
    ventilation_status,
    LAG(ventilation_status) OVER (PARTITION BY stay_id ORDER BY charttime) AS ventilation_status_lag,
    CASE
      WHEN LAG(ventilation_status) OVER (PARTITION BY stay_id ORDER BY charttime) IS NULL THEN 1
      WHEN LAG(ventilation_status) OVER (PARTITION BY stay_id ORDER BY charttime) <> ventilation_status THEN 1
      ELSE 0
    END AS new_event
  FROM classify
  WHERE ventilation_status IS NOT NULL
),

seq AS (
  SELECT
    stay_id,
    charttime,
    ventilation_status,
    SUM(new_event) OVER (PARTITION BY stay_id ORDER BY charttime) AS vent_seq,
    LEAD(charttime) OVER (PARTITION BY stay_id ORDER BY charttime) AS charttime_lead
  FROM mark
)

-- Aggregate contiguous points with the same category into events
SELECT
  stay_id,
  MIN(charttime) AS starttime,
  MAX(COALESCE(charttime_lead, charttime)) AS endtime,
  MAX(ventilation_status) AS ventilation_status
FROM seq
GROUP BY stay_id, vent_seq
HAVING MIN(charttime) <> MAX(COALESCE(charttime_lead, charttime))
ORDER BY stay_id, starttime;