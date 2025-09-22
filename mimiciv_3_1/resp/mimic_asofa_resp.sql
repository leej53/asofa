-- BigQuery Standard SQL
-- Output: <PROJECT>.<DATASET>.mimic_asofa_resp
--
-- Purpose
--   Compute aSOFA-Resp (device + adjunct) and SOFA-Resp (first_day_sofa)
--   for ADULTS (age >= 18) on the FIRST ICU stay, using the first 24h window.
--   Relies only on ventilation_statuses_24h (comma-separated string).
--
-- aSOFA-Resp rules
--   If mechvent = 1:
--     + any adjunct (ECMO, NMB, PRONE, pulmonary vasodilator) → 4
--     + otherwise → 3
--   If mechvent = 0:
--     + includes NonInvasiveVent or HFNC → 2
--     + includes any O2 device (not NONE) → 1
--     + NONE only or no status → 0
--
-- SOFA-Resp
--   Taken directly from the respiration column of the MIT-LCP first_day_sofa (derived table). 
--   Null values were conservatively imputed as 0 to avoid overestimation of hypoxemia.
--
-- Outputs
--   subject_id | stay_id | asofa_resp_score | sofa_resp_score | length_of_stay | mortality_30day

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.mimic_asofa_resp AS
WITH first_icu AS (
  SELECT
    i.subject_id, i.stay_id, i.hadm_id, i.intime, i.outtime,
    ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime) AS rn
  FROM `physionet-data.mimiciv_3_1_icu.icustays` i
  JOIN `physionet-data.mimiciv_3_1_hosp.patients` p
    ON p.subject_id = i.subject_id
  WHERE p.anchor_age >= 18
),
first_stay AS (
  SELECT
    subject_id, stay_id, hadm_id, intime, outtime,
    DATETIME_ADD(intime, INTERVAL 24 HOUR) AS intime_24h
  FROM first_icu
  WHERE rn = 1
),
icu_los AS (
  SELECT
    subject_id, stay_id,
    ROUND(DATETIME_DIFF(outtime, intime, HOUR)/24.0, 2) AS length_of_stay
  FROM first_stay
),
-- OASIS mechvent flag (binary, 1 = invasive ventilation recorded)
oasis_mech AS (
  SELECT fs.subject_id, fs.stay_id, fs.hadm_id, fs.intime,
         COALESCE(o.mechvent, 0) AS mechvent
  FROM first_stay fs
  LEFT JOIN `physionet-data.mimiciv_3_1_derived.oasis` o
    ON fs.stay_id = o.stay_id
),
-- Ventilation states observed within first 24h (string, uppercase, comma-separated)
statuses AS (
  SELECT
    v.subject_id,
    v.stay_id,
    UPPER(COALESCE(v.ventilation_statuses_24h, '')) AS ventilation_statuses_24h
  FROM `<PROJECT>.<DATASET>`.ventilation_statuses_24h v
),
-- Adjunct therapies within [intime, intime+24h)
neuroblock_24h AS (
  SELECT n.stay_id, 1 AS neuroblock
  FROM `physionet-data.mimiciv_3_1_derived.neuroblock` n
  JOIN first_stay fs ON n.stay_id = fs.stay_id
  WHERE n.starttime >= fs.intime AND n.starttime < fs.intime_24h
  GROUP BY n.stay_id
),
prone_24h AS (
  SELECT c.stay_id, 1 AS prone
  FROM `physionet-data.mimiciv_3_1_icu.chartevents` c
  JOIN first_stay fs ON c.stay_id = fs.stay_id
  WHERE LOWER(c.value) = 'prone'
    AND c.charttime >= fs.intime AND c.charttime < fs.intime_24h
  GROUP BY c.stay_id
),
pulm_vaso_24h AS (
  SELECT stay_id, 1 AS pulm_vaso FROM (
    SELECT p.stay_id
    FROM `physionet-data.mimiciv_3_1_icu.procedureevents` p
    JOIN first_stay fs ON p.stay_id = fs.stay_id
    WHERE p.itemid IN (229366, 229996)
      AND p.starttime >= fs.intime AND p.starttime < fs.intime_24h
    UNION ALL
    SELECT c.stay_id
    FROM `physionet-data.mimiciv_3_1_icu.chartevents` c
    JOIN first_stay fs ON c.stay_id = fs.stay_id
    WHERE c.itemid IN (224749, 224750)
      AND c.charttime >= fs.intime AND c.charttime < fs.intime_24h
  )
  GROUP BY stay_id
),
ecmo_24h AS (
  SELECT stay_id, 1 AS ecmo FROM (
    SELECT p.stay_id
    FROM `physionet-data.mimiciv_3_1_icu.procedureevents` p
    JOIN first_stay fs ON p.stay_id = fs.stay_id
    WHERE p.itemid IN (
      224660, 228193, 229266, 229267, 229268, 229269, 229270, 229271, 229272,
      229273, 229274, 229275, 229276, 229277, 229278, 229280, 229363, 229364,
      229365, 229529, 229530, 230159, 230161, 230164, 230168, 230169, 230170, 230171
    )
      AND p.starttime >= fs.intime AND p.starttime < fs.intime_24h
    UNION ALL
    SELECT c.stay_id
    FROM `physionet-data.mimiciv_3_1_icu.chartevents` c
    JOIN first_stay fs ON c.stay_id = fs.stay_id
    WHERE c.itemid IN (
      224660, 228193, 229266, 229267, 229268, 229269, 229270, 229271, 229272,
      229273, 229274, 229275, 229276, 229277, 229278, 229280, 229363, 229364,
      229365, 229529, 229530, 230159, 230161, 230164, 230168, 230169, 230170, 230171
    )
      AND c.charttime >= fs.intime AND c.charttime < fs.intime_24h
  )
  GROUP BY stay_id
),
-- SOFA respiration subscore
sofa_day1 AS (
  SELECT fs.stay_id, COALESCE(fd.respiration, 0) AS sofa_resp_score
  FROM first_stay fs
  LEFT JOIN `physionet-data.mimiciv_3_1_derived.first_day_sofa` fd
    ON fd.stay_id = fs.stay_id
),
-- Assemble features
features AS (
  SELECT
    om.subject_id,
    om.stay_id,
    om.hadm_id,
    om.intime,
    COALESCE(om.mechvent, 0) AS mechvent,
    s.ventilation_statuses_24h,
    COALESCE(nb.neuroblock, 0) AS neuroblock,
    COALESCE(pr.prone, 0)      AS prone,
    COALESCE(pv.pulm_vaso, 0)  AS pulmvaso,
    COALESCE(e.ecmo, 0)        AS ecmo,
    COALESCE(sd.sofa_resp_score, 0) AS sofa_resp_score
  FROM oasis_mech om
  LEFT JOIN statuses s        ON s.subject_id = om.subject_id AND s.stay_id = om.stay_id
  LEFT JOIN neuroblock_24h nb ON nb.stay_id = om.stay_id
  LEFT JOIN prone_24h pr      ON pr.stay_id  = om.stay_id
  LEFT JOIN pulm_vaso_24h pv  ON pv.stay_id  = om.stay_id
  LEFT JOIN ecmo_24h e        ON e.stay_id   = om.stay_id
  LEFT JOIN sofa_day1 sd      ON sd.stay_id  = om.stay_id
),
-- aSOFA scoring based on ventilation_statuses_24h + adjuncts
scored AS (
  SELECT
    f.*,
    CASE
      WHEN f.mechvent = 0 THEN
        CASE
          WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(f.ventilation_statuses_24h, ',')) s
            WHERE TRIM(s) IN ('NONINVASIVEVENT','HFNC')
          ) THEN 2
          WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(f.ventilation_statuses_24h, ',')) s
            WHERE TRIM(s) NOT IN ('','NONE')
          ) THEN 1
          ELSE 0
        END
      ELSE
        CASE
          WHEN f.neuroblock=1 OR f.prone=1 OR f.pulmvaso=1 OR f.ecmo=1 THEN 4
          ELSE 3
        END
    END AS asofa_resp_score
  FROM features f
),
mort_30d AS (
  SELECT
    a.subject_id, a.hadm_id,
    CASE
      WHEN a.deathtime IS NOT NULL
        AND a.deathtime <= TIMESTAMP_ADD(a.admittime, INTERVAL 30 DAY)
        AND a.deathtime <= a.dischtime
      THEN 1 ELSE 0 END AS mortality_30day
  FROM `physionet-data.mimiciv_3_1_hosp.admissions` a
)

-- Final output: consistent headers, no NULLs
SELECT
  s.subject_id,
  s.stay_id,
  COALESCE(s.asofa_resp_score, 0) AS asofa_resp_score,
  COALESCE(s.sofa_resp_score, 0)  AS sofa_resp_score,
  COALESCE(l.length_of_stay, 0)   AS length_of_stay,
  COALESCE(m.mortality_30day, 0)  AS mortality_30day
FROM scored s
LEFT JOIN icu_los l
  ON l.subject_id = s.subject_id AND l.stay_id = s.stay_id
LEFT JOIN mort_30d m
  ON m.subject_id = s.subject_id AND m.hadm_id = s.hadm_id
ORDER BY s.subject_id, s.stay_id;
