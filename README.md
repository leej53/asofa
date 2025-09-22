# asofa
Reproducible SQL/R pipelines for automated SOFA (aSOFA) vs SOFA mortality prediction using MIMIC-IV and eICU-CRD

# aSOFA vs SOFA — Reproducible Pipeline (MIMIC-IV v3.1 + eICU-CRD)

## Quick Start

1. **Set up your environment**  
   - Install R ≥ 4.1 with required packages.  
   - Authenticate Google BigQuery with your PhysioNet-approved account.  
   - Edit each R script to set `USER_EMAIL`, `PROJECT`, `MIMIC_DATASET`, and `EICU_DATASET`.

2. **Run SQL build scripts in BigQuery** (see Section 2; you may first run the optional *population count* scripts to confirm S0–S3 cohort sizes, and the optional *missingness* script for TRIPOD transparency).  
   These create the required `*_asofa_*` and baseline tables in your project.

3. **Run R analysis scripts individually** (see Section 3).  
   Each script produces PNG and CSV outputs in `~/Documents`.  
   Example:
   ```bash
   Rscript R/1a.Baseline_characteristics.R
   Rscript R/2a.Calibration.R
   ```

4. **Check outputs** in your Documents folder. Figures and tables can be inserted directly into manuscripts or presentations.

---

This repository provides a reproducible workflow to build **aSOFA** and **SOFA** tables from BigQuery, analyze 30-day in-hospital mortality, and generate publication-ready figures and tables.

**Datasets**: MIMIC-IV v3.1 (MIT-LCP derived tables) and eICU-CRD  
**Backends**: Google BigQuery (SQL) and R (analysis + visualization)

---

## 1. Prerequisites

- **Google BigQuery** with access to MIMIC-IV v3.1 and eICU-CRD (and `physionet-data.mimiciv_3_1_derived.first_day_sofa` for MIMIC).  
- **R ≥ 4.1** with CRAN access. Required packages are automatically installed if missing:  
  `DBI, bigrquery, dplyr, tidyr, ggplot2, gt, survival, pROC, patchwork, ragg, webshot2, readr, tibble, forcats, stringr, purrr, data.table, DescTools, glue`.  
- **Authentication**: configure Google BigQuery OAuth. Set `options(gargle_oauth_email = USER_EMAIL)` or authenticate interactively.

**User-specific settings (edit in each R script):**
```r
USER_EMAIL    <- "YOUR_EMAIL_HERE"
PROJECT       <- "YOUR_PROJECT_ID"
MIMIC_DATASET <- "YOUR_MIMIC_DATASET"   # e.g., "mimic_output"
EICU_DATASET  <- "YOUR_EICU_DATASET"    # e.g., "eicu_output"
```

---


---
## 0. (Optional) Cohort Population Counts

Before building aSOFA tables, you can generate reproducible cohort headcounts to report S0–S3 filters:

- `mimic_population_counts.sql` → creates `<PROJECT>.<DATASET>.mimic_population_counts`
- `eicu_population_counts.sql` → creates `<PROJECT>.<DATASET>.eicu_population_counts`

**Stages**
- **S0**: All persons/stays
- **S1**: Non-missing age & sex
- **S2**: Adults (≥18 years; eICU treats `> 89` as 90 for checks)
- **S3**: Adults with **first ICU** only (aligns with day‑1 feature base)
## 2. SQL Build Order

Run the SQL scripts in the following order. They create all source and derived tables required for downstream R analyses.

**Pre-checks (optional but recommended)**
0. `mimic_population_counts.sql` — person-level S0–S3 counts (MIMIC)
0. `eicu_population_counts.sql` — stay-level S0–S3 counts (eICU)

**Mapping and utility tables**
1. `eicu_resp_support_map.sql` — respiratory device/mode mapping  
2. `eicu_vasopressors_map.sql` — vasopressor mapping  
3. `eicu_charlson_map.sql` — Charlson comorbidity mapping

**Source-derived 24h summaries**
4. `mimic_ventilation.sql` / `mimic_ventilation_statuses_24h.sql`  
5. `mimic_ventilator_setting.sql`  
6. `eicu_asofa_resp.sql`, `eicu_asofa_cv.sql`, `eicu_asofa_renal.sql` (24h joiners for eICU)

**aSOFA subscores**
7. `mimic_asofa_resp.sql`  
8. `mimic_asofa_cv.sql`  
9. `mimic_asofa_renal.sql`  
10. `eicu_asofa_resp.sql`  
11. `eicu_asofa_cv.sql`  
12. `eicu_asofa_renal.sql`

**Totals**
13. `mimic_asofa_total.sql`  
14. `eicu_asofa_total.sql`

**Sepsis-3**
15. `mimic_suspicion_of_infection.sql`  
16. `mimic_sepsis3.sql`  
17. `eicu_sepsis3.sql`

**Sepsis-restricted totals**
18. `mimic_asofa_total_sepsis.sql`  
19. `eicu_asofa_total_sepsis.sql`

**Baseline and diagnosis groups**
20. `mimic_first_icu_dx_groups.sql`  
21. `eicu_first_icu_dx_groups.sql`  
22. `mimic_baseline_characteristics.sql`  
23. `eicu_baseline_characteristics.sql`  
24. `mimic_baseline_characteristics_sepsis.sql`  
25. `eicu_baseline_characteristics_sepsis.sql`

Most scripts use `CREATE OR REPLACE TABLE`, making re-runs safe.

**Diagnostics / Transparency (optional)**
26. `missingness_day1_side_by_side.sql` — creates `<PROJECT>.<OUTPUT_DATASET>.missingness_day1_side_by_side`; requires `mimic_ventilation` to exist; used by R script 3.8.


---

## 3. R Analyses (run individually)

Each R script connects directly to BigQuery, queries the required tables, and outputs results (PNG/CSV) to `~/Documents`.  
Due to computational load, it is recommended to run **each script separately** rather than all at once.

### 3.1 Baseline characteristics
`R/1a.Baseline_characteristics.R`  
- Side-by-side comparison (MIMIC vs eICU) with standardized mean differences  
- Outputs: `baseline_side_by_side.png`, `baseline_side_by_side.csv`

### 3.2 Baseline diagnosis groups
`R/1b.Baseline_characteristics_dx_groups.R`  
- ICU admission diagnosis group distributions with SMD  
- Outputs: `dxgroups_minimal_table.png`, `mimic_eicu_dxgroups_with_smd.csv`

### 3.3 Calibration plots
`R/2a.Calibration.R`  
- 4-in-1 panel plots comparing aSOFA vs SOFA predictions  
- Outputs: `calibration_4in1_asofa_vs_sofa.png`

### 3.4 Calibration summary statistics
`R/2b.Calibration_summary_statistics.R`  
- Brier score (bootstrap CI), calibration-in-the-large, slope  
- Outputs: `calibration_metrics_table.png`, `supplement_brier_skill_table.csv/png`

### 3.5 Cox proportional hazards models
`R/3.Cox.R`  
- Hazard ratios, C-index, ΔC (bootstrap CI), forest plots, tables  
- Outputs: `cox_hr_forest.png`, `discrimination_table.png`, `discrimination_results.csv`

### 3.6 ROC and AUC comparison
`R/4.ROC.R`  
- ROC curves (2-in-1 panel), DeLong test, ΔAUC bootstrap CI  
- Outputs: `roc_panel_asofa_vs_sofa.png`, `auc_delong_table.png`, `auc_delong_results.csv`

### 3.7 aSOFA–SOFA discrepancy and agreement
`R/5.Discrepancy.R`  
- Bland–Altman plots, weighted kappa for CV/Resp/Renal subscores  
- Outputs: `ba_panel_asofa_vs_sofa.png`, `ba_summary_table.png/csv`, `kappa_weighted_table.png/csv`

### 3.8 First‑24h Missingness & Timing Artifacts
`R/6.Missingness_day1_side_by_side.R`
- Queries `<PROJECT>.<OUTPUT_DATASET>.missingness_day1_side_by_side` and builds a side‑by‑side table
- Columns: SOFA Variable | MIMIC‑IV: n (%) | eICU‑CRD: n (%) | Handling (pre‑specified)
- Outputs: `missingness_day1_side_by_side.png`, `missingness_day1_side_by_side.csv`

---

## 5. Reproducibility notes

- Environment variables (`PROJECT`, `MIMIC_DATASET`, `EICU_DATASET`, `USER_EMAIL`) can be set globally in `.Renviron` or `.bashrc`.  
- Outputs are saved to `~/Documents` by default. Paths can be changed in each script.  
- Random seeds are fixed for reproducibility in bootstrap steps.  
- Some analyses (e.g., bootstrap B=1000) are computationally heavy; run scripts individually.

---

## 6. License & Citation

- All code is released for academic reproducibility.  
- Please cite the corresponding aSOFA manuscript and this repository if used.  
- Data use must comply with PhysioNet/MIT-LCP/eICU-CRD licensing and your IRB/DUA.
