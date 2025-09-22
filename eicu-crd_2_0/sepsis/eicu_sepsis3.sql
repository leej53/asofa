-- ============================================================
-- eICU | Sepsis-3 (first ICU stay, adults) — SOFA aligned with aSOFA modules
-- Output: `<PROJECT>.<DATASET>`.sepsis3
--
-- Key alignment (matches aSOFA/SOFA component code used elsewhere):
-- - Window: 0..1440 minutes from ICU admission for all inputs
-- - Resp SOFA: PF-based with standard cutoffs; vent=0 & PF<200 => at least 2 points
-- - CV SOFA: dose-based (dopamine/dobutamine/epi/NE + phenylephrine/10),
--            MAP rule if no pressors; if all MAP missing and no pressors, impute 71 mmHg -> 0pt
-- - Renal/Coag/Liver SOFA: same thresholds as in domain modules
-- - CNS: included here for Sepsis-3 determination (excluded from aSOFA totals elsewhere)
-- - Suspected infection: antibiotics ± infection diagnoses (Seymour-style temporal logic)
-- ============================================================

CREATE OR REPLACE TABLE `<PROJECT>.<DATASET>`.sepsis3 AS
WITH
-- -------------------------
-- 0) Cohort: first ICU stay (adults)
-- -------------------------
icu AS (
  SELECT
    icu.patientunitstayid,
    icu.unitvisitnumber,
    CASE WHEN icu.age = '> 89' THEN 90 ELSE SAFE_CAST(icu.age AS INT64) END AS age,
    icu.gender
  FROM `physionet-data.eicu_crd_derived.icustay_detail` AS icu
  WHERE icu.age IS NOT NULL
    AND icu.gender IS NOT NULL
    AND icu.unitvisitnumber = 1
    AND (icu.age = '> 89' OR SAFE_CAST(icu.age AS INT64) >= 18)
),

-- -------------------------
-- 1) Antibiotics (medication ∪ infusiondrug)
-- -------------------------
ab_from_med AS (
  SELECT
    m.patientunitstayid,
    LOWER(m.drugname) AS rawname,
    m.drugstartoffset AS ab_offset
  FROM `physionet-data.eicu_crd.medication` m
  JOIN icu i ON m.patientunitstayid = i.patientunitstayid
  WHERE m.drugstartoffset IS NOT NULL
),
ab_from_inf AS (
  SELECT
    i2.patientunitstayid,
    LOWER(i2.drugname) AS rawname,
    i2.infusionoffset AS ab_offset
  FROM `physionet-data.eicu_crd.infusiondrug` i2
  JOIN icu i ON i2.patientunitstayid = i.patientunitstayid
  WHERE i2.infusionoffset IS NOT NULL
),
ab_all AS (
  SELECT * FROM ab_from_med
  UNION ALL
  SELECT * FROM ab_from_inf
),
-- canonical antibiotic name mapping (same patterns as our modular codebase)
ab_tbl AS (
  SELECT
    patientunitstayid,
    ab_offset,
    CASE
      WHEN REGEXP_CONTAINS(rawname, r'\b(zosyn|piperacillin|tazobactam)\b') THEN 'piperacillin-tazobactam'
      WHEN REGEXP_CONTAINS(rawname, r'\b(ampicillin-sulbactam|unasyn)\b') THEN 'ampicillin-sulbactam'
      WHEN REGEXP_CONTAINS(rawname, r'\b(amoxicillin\-clav|augmentin)\b') THEN 'amoxicillin-clavulanate'
      WHEN REGEXP_CONTAINS(rawname, r'\b(ceftriaxone)\b') THEN 'ceftriaxone'
      WHEN REGEXP_CONTAINS(rawname, r'\b(cefepime)\b') THEN 'cefepime'
      WHEN REGEXP_CONTAINS(rawname, r'\b(cefotaxime)\b') THEN 'cefotaxime'
      WHEN REGEXP_CONTAINS(rawname, r'\b(ceftazidime)\b') THEN 'ceftazidime'
      WHEN REGEXP_CONTAINS(rawname, r'\b(ceftaroline)\b') THEN 'ceftaroline'
      WHEN REGEXP_CONTAINS(rawname, r'\b(cefuroxime)\b') THEN 'cefuroxime'
      WHEN REGEXP_CONTAINS(rawname, r'\b(cefazolin)\b') THEN 'cefazolin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(aztreonam)\b') THEN 'aztreonam'
      WHEN REGEXP_CONTAINS(rawname, r'\b(meropenem)\b') THEN 'meropenem'
      WHEN REGEXP_CONTAINS(rawname, r'\b(imipenem|cilastatin)\b') THEN 'imipenem'
      WHEN REGEXP_CONTAINS(rawname, r'\b(ertapenem)\b') THEN 'ertapenem'
      WHEN REGEXP_CONTAINS(rawname, r'\b(doripenem)\b') THEN 'doripenem'
      WHEN REGEXP_CONTAINS(rawname, r'\b(vancomycin)\b') THEN 'vancomycin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(linezolid)\b') THEN 'linezolid'
      WHEN REGEXP_CONTAINS(rawname, r'\b(daptomycin)\b') THEN 'daptomycin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(gentamicin)\b') THEN 'gentamicin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(tobramycin)\b') THEN 'tobramycin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(amikacin)\b') THEN 'amikacin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(ciprofloxacin)\b') THEN 'ciprofloxacin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(levofloxacin)\b') THEN 'levofloxacin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(moxifloxacin)\b') THEN 'moxifloxacin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(azithromycin)\b') THEN 'azithromycin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(clarithromycin)\b') THEN 'clarithromycin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(erythromycin)\b') THEN 'erythromycin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(clindamycin)\b') THEN 'clindamycin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(metronidazole)\b') THEN 'metronidazole'
      WHEN REGEXP_CONTAINS(rawname, r'\b(tigecycline)\b') THEN 'tigecycline'
      WHEN REGEXP_CONTAINS(rawname, r'\b(colistin|polymyxin)\b') THEN 'colistin/polymyxin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(doxycycline|minocycline|tetracycline)\b') THEN 'tetracyclines'
      WHEN REGEXP_CONTAINS(rawname, r'\b(trimethoprim|sulfamethoxazole|bactrim|septra)\b') THEN 'tmp-smx'
      WHEN REGEXP_CONTAINS(rawname, r'\b(rifampin|rifampicin)\b') THEN 'rifampin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(nitrofurantoin)\b') THEN 'nitrofurantoin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(fosfomycin)\b') THEN 'fosfomycin'
      WHEN REGEXP_CONTAINS(rawname, r'\b(penicillin|nafcillin|oxacillin|dicloxacillin)\b') THEN 'penicillins'
      ELSE NULL
    END AS antibiotic
  FROM ab_all
  WHERE REGEXP_CONTAINS(rawname, r'zosyn|piperacillin|tazobactam|unasyn|ampicillin|amoxicillin|augmentin|cef|aztreonam|meropenem|imipenem|ertapenem|doripenem|vancomycin|linezolid|daptomycin|gentamicin|tobramycin|amikacin|ciprofloxacin|levofloxacin|moxifloxacin|azithromycin|clarithromycin|erythromycin|clindamycin|metronidazole|tigecycline|colistin|polymyxin|doxycycline|minocycline|tetracycline|trimethoprim|sulfamethoxazole|bactrim|septra|rifampin|rifampicin|nitrofurantoin|fosfomycin|nafcillin|oxacillin|dicloxacillin|penicillin')
),
ab_tbl_numbered AS (
  SELECT
    patientunitstayid, antibiotic, ab_offset,
    ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY ab_offset) AS ab_id
  FROM ab_tbl
),

-- -------------------------
-- 2) Dx-as-culture proxy (expanded infection diagnoses)
-- -------------------------
diag_raw AS (
  SELECT
    d.patientunitstayid,
    d.diagnosisoffset AS dx_offset,
    UPPER(IFNULL(d.diagnosisstring, '')) AS diagnosisstring_up,
    REGEXP_REPLACE(UPPER(IFNULL(d.icd9code, '')), r'[\s|;]+', ',') AS icd_codes_norm
  FROM `physionet-data.eicu_crd.diagnosis` d
  JOIN icu i ON d.patientunitstayid = i.patientunitstayid
  WHERE d.diagnosisoffset IS NOT NULL
),
codes AS (
  SELECT
    patientunitstayid, dx_offset,
    REGEXP_REPLACE(TRIM(code), r'[^A-Z0-9]', '') AS icd_code,
    diagnosisstring_up
  FROM diag_raw, UNNEST(SPLIT(icd_codes_norm, ',')) AS code
  WHERE TRIM(code) <> ''
),
text_only AS (
  SELECT DISTINCT
    patientunitstayid, dx_offset,
    CAST(NULL AS STRING) AS icd_code,
    diagnosisstring_up
  FROM diag_raw
  WHERE icd_codes_norm IS NULL OR icd_codes_norm = ''
),
dx_union AS (
  SELECT * FROM codes
  UNION ALL
  SELECT * FROM text_only
),

-- Infection filters (ICD + text), expanded like our earlier module
dx_micro_proxy AS (
  SELECT DISTINCT patientunitstayid, dx_offset
  FROM dx_union
  WHERE
    -- Sepsis/Septic shock
    (icd_code LIKE '038%' OR icd_code IN ('99591','99592','78552')
     OR icd_code LIKE 'A40%' OR icd_code LIKE 'A41%' OR icd_code IN ('R6520','R6521'))
    OR
    -- Pneumonia
    (REGEXP_CONTAINS(icd_code, r'^(480|481|482|483|484|485|486)\d*$')
     OR icd_code LIKE '4870%' OR icd_code LIKE 'J12%' OR icd_code LIKE 'J13%' OR
     icd_code LIKE 'J14%' OR icd_code LIKE 'J15%' OR icd_code LIKE 'J16%' OR
     icd_code LIKE 'J17%' OR icd_code LIKE 'J18%')
    OR
    -- UTI/Pyelo/Cystitis
    (icd_code LIKE '5990%' OR icd_code LIKE '590%' OR icd_code LIKE '595%'
     OR icd_code LIKE 'N390%' OR icd_code LIKE 'N10%' OR icd_code LIKE 'N12%' OR icd_code LIKE 'N30%')
    OR
    -- Biliary
    (icd_code LIKE '5761%' OR icd_code LIKE '5750%' OR icd_code LIKE '5751%'
     OR icd_code LIKE 'K830%' OR icd_code LIKE 'K81%')
    OR
    -- Intra-abdominal
    (icd_code LIKE '567%' OR icd_code LIKE '540%' OR icd_code LIKE '541%'
     OR icd_code LIKE 'K65%' OR icd_code LIKE 'K35%' OR icd_code LIKE 'K36%' OR icd_code LIKE 'K37%')
    OR
    -- Skin/Soft tissue
    (icd_code LIKE '682%' OR icd_code LIKE '0411%' OR icd_code LIKE '72886%'
     OR icd_code LIKE 'L03%' OR icd_code LIKE 'L02%' OR icd_code LIKE 'L08%' OR icd_code LIKE 'M726%')
    OR
    -- Osteomyelitis
    (icd_code LIKE '730%' OR icd_code LIKE 'M86%')
    OR
    -- Endocarditis
    (icd_code LIKE '421%' OR icd_code LIKE 'I33%')
    OR
    -- Meningitis
    (icd_code LIKE '320%' OR icd_code LIKE '322%' OR icd_code LIKE 'G00%')
    OR
    -- Septic arthritis
    (icd_code LIKE '7110%' OR icd_code LIKE 'M00%')
    OR
    -- Bacteremia
    (icd_code LIKE '7907%' OR icd_code LIKE 'R7881%')
    OR
    -- Device/catheter-related infection
    (icd_code LIKE '9966%' OR icd_code LIKE '99931%'
     OR icd_code LIKE 'T827%' OR icd_code LIKE 'T835%' OR icd_code LIKE 'T845%' OR icd_code LIKE 'T857%')
    OR
    -- C. difficile colitis
    (icd_code LIKE 'A047%')
    OR
    -- Text backups
    REGEXP_CONTAINS(
      diagnosisstring_up,
      r'\bSEPSIS\b|SEPTIC SHOCK|\bSEPTIC\b|\bPNEUMONIA\b|\bPNA\b|'
      r'\bUTI\b|URINARY TRACT INFECTION|PYELONEPHRITIS|CYSTITIS|'
      r'CHOLANGITIS|CHOLECYSTITIS|PERITONITIS|APPENDICITIS|INTRA[- ]?ABDOMINAL|'
      r'CELLULITIS|ABSCESS|NEC(ROTIZING)?\s+FASCIITIS|SSTI|'
      r'OSTEOMYELITIS|ENDOCARDITIS|MENINGITIS|SEPTIC ARTHRITIS|'
      r'BACTEREMIA|C(\.| )?DIFF(ICILE)?|CLOSTRIDI(UM|OIDE)S'
    )
),

-- -------------------------
-- 3) Suspicion-of-infection pairing (Seymour logic)
-- antibiotics within 72h after OR 24h before infection-related Dx
-- -------------------------
dx_then_ab AS (
  SELECT
    a.patientunitstayid, a.ab_id, a.antibiotic, a.ab_offset,
    x.dx_offset AS last72_dx_offset,
    ROW_NUMBER() OVER (PARTITION BY a.patientunitstayid, a.ab_id ORDER BY x.dx_offset) AS rn
  FROM ab_tbl_numbered a
  JOIN dx_micro_proxy x
    ON a.patientunitstayid = x.patientunitstayid
   AND x.dx_offset BETWEEN a.ab_offset AND a.ab_offset + 72*60
),
ab_then_dx AS (
  SELECT
    a.patientunitstayid, a.ab_id, a.antibiotic, a.ab_offset,
    x.dx_offset AS next24_dx_offset,
    ROW_NUMBER() OVER (PARTITION BY a.patientunitstayid, a.ab_id ORDER BY x.dx_offset DESC) AS rn
  FROM ab_tbl_numbered a
  JOIN dx_micro_proxy x
    ON a.patientunitstayid = x.patientunitstayid
   AND x.dx_offset BETWEEN a.ab_offset - 24*60 AND a.ab_offset - 1
),
suspicion_per_ab AS (
  SELECT
    a.patientunitstayid, a.ab_id, a.antibiotic,
    a.ab_offset AS antibiotic_offset,
    CASE WHEN l.last72_dx_offset IS NULL AND n.next24_dx_offset IS NULL THEN 0 ELSE 1 END AS suspected_infection,
    CASE WHEN l.last72_dx_offset IS NULL AND n.next24_dx_offset IS NULL THEN NULL
         ELSE COALESCE(l.last72_dx_offset, a.ab_offset) END AS suspected_infection_offset,
    COALESCE(l.last72_dx_offset, n.next24_dx_offset) AS culture_proxy_offset
  FROM ab_tbl_numbered a
  LEFT JOIN (SELECT * FROM dx_then_ab WHERE rn = 1) l
    ON a.patientunitstayid = l.patientunitstayid AND a.ab_id = l.ab_id
  LEFT JOIN (SELECT * FROM ab_then_dx WHERE rn = 1) n
    ON a.patientunitstayid = n.patientunitstayid AND a.ab_id = n.ab_id
),
-- Earliest suspicion per stay
suspicion AS (
  SELECT AS VALUE s
  FROM (
    SELECT
      patientunitstayid, antibiotic, antibiotic_offset,
      suspected_infection, suspected_infection_offset, culture_proxy_offset,
      ROW_NUMBER() OVER (
        PARTITION BY patientunitstayid
        ORDER BY suspected_infection DESC, COALESCE(suspected_infection_offset, 9e9)
      ) AS rn
    FROM suspicion_per_ab
  ) s
  WHERE rn = 1
),

-- -------------------------
-- 4) First-day SOFA (apacheapsvar + labsfirstday + vaso mcg/kg/min)
-- *** Aligned with domain modules; phenylephrine NE-equivalent; MAP=71 if entirely missing and no pressors ***
-- -------------------------
aps AS (
  SELECT
    a.patientunitstayid, a.intubated, a.vent, a.dialysis,
    a.eyes, a.motor, a.verbal, a.urine, a.wbc, a.temperature,
    a.respiratoryrate, a.sodium, a.heartrate, a.meanbp,
    a.ph, a.hematocrit, a.creatinine, a.albumin,
    a.pao2, a.pco2, a.bun, a.glucose, a.bilirubin, a.fio2
  FROM `physionet-data.eicu_crd.apacheapsvar` a
  JOIN icu i ON a.patientunitstayid = i.patientunitstayid
),
platelet_min AS (
  SELECT l.patientunitstayid, l.PLATELET_min
  FROM `physionet-data.eicu_crd_derived.labsfirstday` l
  JOIN icu i ON l.patientunitstayid = i.patientunitstayid
  WHERE l.PLATELET_min IS NOT NULL
),
-- mcg/kg/min standardization (same as CV module)
vaso_raw AS (
  SELECT
    i3.patientunitstayid,
    CASE
      WHEN LOWER(i3.drugname) LIKE '%norepinephrine%' THEN 'norepinephrine'
      WHEN LOWER(i3.drugname) LIKE '%epinephrine%'     THEN 'epinephrine'
      WHEN LOWER(i3.drugname) LIKE '%dopamine%'        THEN 'dopamine'
      WHEN LOWER(i3.drugname) LIKE '%dobutamine%'      THEN 'dobutamine'
      WHEN LOWER(i3.drugname) LIKE '%phenylephrine%'   THEN 'phenylephrine'
      ELSE NULL
    END AS drug,
    CASE
      WHEN LOWER(i3.drugname) LIKE '%mcg/kg/min%' THEN SAFE_CAST(i3.drugrate AS FLOAT64)
      ELSE SAFE_CAST(i3.drugrate AS FLOAT64) / NULLIF(p.admissionweight, 0)
    END AS drugrate_adj
  FROM `physionet-data.eicu_crd.infusiondrug` i3
  LEFT JOIN `physionet-data.eicu_crd.patient` p
    ON i3.patientunitstayid = p.patientunitstayid
  JOIN icu i ON i3.patientunitstayid = i.patientunitstayid
  WHERE i3.infusionoffset BETWEEN 0 AND 1440
    AND i3.drugrate IS NOT NULL AND SAFE_CAST(i3.drugrate AS FLOAT64) != 0
    AND (
      LOWER(i3.drugname) LIKE '%norepinephrine%' OR
      LOWER(i3.drugname) LIKE '%epinephrine%' OR
      LOWER(i3.drugname) LIKE '%dopamine%' OR
      LOWER(i3.drugname) LIKE '%dobutamine%' OR
      LOWER(i3.drugname) LIKE '%phenylephrine%'
    )
),
vaso AS (
  SELECT
    patientunitstayid,
    ROUND(MAX(IF(drug='norepinephrine', drugrate_adj, NULL)), 3) AS norepinephrine,
    ROUND(MAX(IF(drug='epinephrine',    drugrate_adj, NULL)), 3) AS epinephrine,
    ROUND(MAX(IF(drug='dopamine',       drugrate_adj, NULL)), 3) AS dopamine,
    ROUND(MAX(IF(drug='dobutamine',     drugrate_adj, NULL)), 3) AS dobutamine,
    ROUND(MAX(IF(drug='phenylephrine',  drugrate_adj, NULL)), 3) AS phenylephrine
  FROM vaso_raw
  GROUP BY patientunitstayid
),
map_min_raw AS (
  SELECT
    v.patientunitstayid,
    MIN(v.ibp_mean)  AS ibp_min,
    MIN(v.nibp_mean) AS nibp_min
  FROM `physionet-data.eicu_crd_derived.pivoted_vital` v
  JOIN icu i ON v.patientunitstayid = i.patientunitstayid
  WHERE v.entryoffset BETWEEN 0 AND 1440
  GROUP BY v.patientunitstayid
),
-- Impute MAP=71 when entirely missing and no pressors (prevents spurious SOFA=1)
map_min AS (
  SELECT
    m.patientunitstayid,
    CASE
      WHEN m.ibp_min IS NOT NULL THEN m.ibp_min
      WHEN m.nibp_min IS NOT NULL THEN m.nibp_min
      ELSE 71.0
    END AS map_min_imputed
  FROM map_min_raw m
),
sofa_scored AS (
  SELECT
    i.patientunitstayid AS patientunitstayid,

    -- Renal (creatinine)
    CASE
      WHEN a.creatinine IS NULL OR a.creatinine = -1 OR a.creatinine < 1.2 THEN 0
      WHEN a.creatinine < 2.0 THEN 1
      WHEN a.creatinine < 3.5 THEN 2
      WHEN a.creatinine < 5.0 THEN 3
      ELSE 4
    END AS renal,

    -- Coagulation (platelet)
    CASE
      WHEN l.PLATELET_min IS NULL OR l.PLATELET_min >= 150 THEN 0
      WHEN l.PLATELET_min >= 100 THEN 1
      WHEN l.PLATELET_min >= 50  THEN 2
      WHEN l.PLATELET_min >= 20  THEN 3
      ELSE 4
    END AS coagulation,

    -- Liver (bilirubin)
    CASE
      WHEN a.bilirubin IS NULL OR a.bilirubin = -1 OR a.bilirubin < 1.2 THEN 0
      WHEN a.bilirubin < 2.0  THEN 1
      WHEN a.bilirubin < 6.0  THEN 2
      WHEN a.bilirubin < 12.0 THEN 3
      ELSE 4
    END AS liver,

    -- Respiration (PaO2/FiO2)
    CASE
      WHEN a.pao2 IS NULL OR a.fio2 IS NULL THEN 0
      ELSE CASE
        WHEN a.vent = 1 AND (a.pao2 / (CASE WHEN a.fio2 > 1 THEN a.fio2/100 ELSE NULLIF(a.fio2,0) END)) < 100 THEN 4
        WHEN a.vent = 1 AND (a.pao2 / (CASE WHEN a.fio2 > 1 THEN a.fio2/100 ELSE NULLIF(a.fio2,0) END)) < 200 THEN 3
        WHEN (a.pao2 / (CASE WHEN a.fio2 > 1 THEN a.fio2/100 ELSE NULLIF(a.fio2,0) END)) < 300 THEN 2
        WHEN (a.pao2 / (CASE WHEN a.fio2 > 1 THEN a.fio2/100 ELSE NULLIF(a.fio2,0) END)) < 400 THEN 1
        ELSE 0
      END
    END AS respiration,

    -- CNS (GCS)
    CASE
      WHEN a.eyes IS NULL OR a.motor IS NULL OR a.verbal IS NULL THEN 0
      ELSE CASE
        WHEN (a.eyes + a.motor + a.verbal) >= 15 THEN 0
        WHEN (a.eyes + a.motor + a.verbal) >= 13 THEN 1
        WHEN (a.eyes + a.motor + a.verbal) >= 10 THEN 2
        WHEN (a.eyes + a.motor + a.verbal) >= 6  THEN 3
        ELSE 4
      END
    END AS cns,

    -- Cardiovascular (dose-based with phenylephrine NE-equivalent; MAP rule if no pressors)
    CASE
      WHEN COALESCE(v.dopamine,0) > 15
        OR COALESCE(v.epinephrine,0)  > 0.1
        OR COALESCE(v.norepinephrine,0) > 0.1
        OR (COALESCE(v.phenylephrine,0)/10.0) > 0.1 THEN 4
      WHEN COALESCE(v.dopamine,0) > 5
        OR (COALESCE(v.epinephrine,0)  > 0 AND COALESCE(v.epinephrine,0)  <= 0.1)
        OR (COALESCE(v.norepinephrine,0) > 0 AND COALESCE(v.norepinephrine,0) <= 0.1)
        OR ((COALESCE(v.phenylephrine,0)/10.0) > 0 AND (COALESCE(v.phenylephrine,0)/10.0) <= 0.1) THEN 3
      WHEN (COALESCE(v.dopamine,0) > 0 AND COALESCE(v.dopamine,0) <= 5)
        OR COALESCE(v.dobutamine,0) > 0 THEN 2
      WHEN COALESCE(v.dopamine,0) = 0
        AND COALESCE(v.epinephrine,0) = 0
        AND COALESCE(v.norepinephrine,0) = 0
        AND COALESCE(v.phenylephrine,0) = 0
        AND map.map_min_imputed < 70 THEN 1
      ELSE 0
    END AS cardiovascular
  FROM icu i
  LEFT JOIN aps a          ON a.patientunitstayid = i.patientunitstayid
  LEFT JOIN platelet_min l ON l.patientunitstayid = i.patientunitstayid
  LEFT JOIN vaso v         ON v.patientunitstayid = i.patientunitstayid
  LEFT JOIN map_min map    ON map.patientunitstayid = i.patientunitstayid
),
sofa_1d AS (
  SELECT
    patientunitstayid,
    CAST(respiration AS INT64)    AS respiration,
    CAST(coagulation AS INT64)    AS coagulation,
    CAST(liver       AS INT64)    AS liver,
    CAST(cardiovascular AS INT64) AS cardiovascular,
    CAST(cns         AS INT64)    AS cns,
    CAST(renal       AS INT64)    AS renal,
    SAFE_CAST(
      COALESCE(respiration,0) + COALESCE(coagulation,0) + COALESCE(liver,0) +
      COALESCE(cardiovascular,0) + COALESCE(cns,0) + COALESCE(renal,0)
      AS INT64
    ) AS sofa_score
  FROM sofa_scored
),

-- -------------------------
-- 5) Final: Sepsis-3 + convenience flags
-- -------------------------
final AS (
  SELECT
    i.patientunitstayid,
    0 AS sofa_window_start_offset_min,
    1440 AS sofa_window_end_offset_min,
    s.antibiotic, s.antibiotic_offset,
    s.suspected_infection, s.suspected_infection_offset, s.culture_proxy_offset,
    s1.sofa_score, s1.respiration, s1.coagulation, s1.liver, s1.cardiovascular, s1.cns, s1.renal,
    (s1.sofa_score >= 2 AND s.suspected_infection = 1) AS sepsis3,
    (s1.sofa_score >= 2 AND s.suspected_infection = 1 AND s.suspected_infection_offset <= 0) AS septic_on_arrival,
    (s1.sofa_score >= 2 AND s.suspected_infection = 1 AND s.suspected_infection_offset BETWEEN 0 AND 1440) AS sepsis_within_24h
  FROM icu i
  LEFT JOIN sofa_1d s1 ON s1.patientunitstayid = i.patientunitstayid
  LEFT JOIN suspicion s ON s.patientunitstayid = i.patientunitstayid
)

SELECT * FROM final;
