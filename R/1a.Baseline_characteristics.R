# =====================================================================
# File: 1a.Baseline_characteristics.R
#
# What this script does (for the README header)
# ---------------------------------------------
# Builds a side-by-side baseline characteristics table comparing MIMIC-IV
# vs eICU-CRD, each shown for the Total ICU population and the Sepsis
# (within first 24h/on arrival) subgroup. The table columns are:
#
#   Variable | MIMIC (All) | eICU (All) | SMD (All) | [spacer] |
#             MIMIC (Sepsis) | eICU (Sepsis) | SMD (Sepsis)
#
# Continuous variables are reported as median [IQR]:
#   Age, Age-adjusted Charlson Index (ACCI), aSOFA, SOFA, ICU LOS.
# Binary variables are reported as percentages:
#   sex (Male) and Charlson component flags, plus 30-day in-hospital mortality.
#
# How to use
# ----------
# 1) Edit the placeholders below:
#      USER_EMAIL    <- "YOUR_EMAIL_HERE"
#      PROJECT       <- "YOUR_PROJECT_ID"
#      MIMIC_DATASET <- "YOUR_MIMIC_DATASET"   # e.g., "mimic_output"
#      EICU_DATASET  <- "YOUR_EICU_DATASET"    # e.g., "eicu_output"
#
# 2) Make sure you have built the following tables with the provided SQL:
#      <PROJECT>.<MIMIC_DATASET>.mimic_baseline_characteristics
#      <PROJECT>.<MIMIC_DATASET>.mimic_baseline_characteristics_sepsis
#      <PROJECT>.<EICU_DATASET>.eicu_baseline_characteristics
#      <PROJECT>.<EICU_DATASET>.eicu_baseline_characteristics_sepsis
#    (Schemas/column names follow the released SQL scripts.)
#
# 3) Notes on columns
#    - Columns used here include:
#        age, gender (1=male,0=otherwise), length_of_stay,
#        asofa_total_score, sofa_total_score, charlson_comorbidity_index,
#        mortality_30day (for eICU; for MIMIC we join from totals).
#      Component flags (e.g., myocardial_infarct, renal_disease, etc.) are
#      included as 0/1 indicators in all baseline tables.
#
# 4) Output
#    - PNG and CSV are saved to ~/Documents.
#    - The gt table layout/width/height is preserved from the original script.
# =====================================================================

suppressPackageStartupMessages({
  req <- c("DBI","bigrquery","dplyr","tidyr","readr","glue","gt","stringr","purrr","tibble")
  to_install <- req[!sapply(req, require, character.only = TRUE)]
  if (length(to_install)) install.packages(to_install, Ncpus = 2)
  invisible(lapply(req, library, character.only = TRUE))
})

set.seed(123)

# ---- user-editable defaults ----
USER_EMAIL    <- "YOUR_EMAIL_HERE"
PROJECT       <- "YOUR_PROJECT_ID"
MIMIC_DATASET <- "YOUR_MIMIC_DATASET"  # e.g., "mimic_output"
EICU_DATASET  <- "YOUR_EICU_DATASET"   # e.g., "eicu_output"
# ---------------------------------

# ---- outputs (PNG & CSV only) ----
OUT_DIR  <- file.path(path.expand("~"), "Documents")
PNG_FILE <- file.path(OUT_DIR, "baseline_side_by_side.png")
CSV_FILE <- file.path(OUT_DIR, "baseline_side_by_side.csv")
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

# ---- BigQuery connection ----
options(gargle_oauth_email = USER_EMAIL, gargle_oauth_cache = TRUE)
con <- DBI::dbConnect(
  bigrquery::bigquery(),
  project = PROJECT,
  billing = PROJECT,
  use_legacy_sql = FALSE
)

# ---- pulls (All vs Sepsis) ----
# Table names follow the released SQL:
# - MIMIC: mimic_baseline_characteristics / mimic_baseline_characteristics_sepsis
# - eICU : eicu_baseline_characteristics  / eicu_baseline_characteristics_sepsis
# Columns used: age, gender, length_of_stay, asofa_total_score, sofa_total_score,
#               charlson_comorbidity_index, mortality_30day(+ components)
# Note: mortality_30day is added to MIMIC via join from totals.
sql_mimic_all <- glue::glue("
WITH base AS (
  SELECT
    b.subject_id, b.stay_id,
    CAST(b.age AS FLOAT64)                        AS age,
    SAFE_CAST(b.gender AS INT64)                  AS gender,
    CAST(b.length_of_stay AS FLOAT64)             AS length_of_stay,
    CAST(b.asofa_total_score AS FLOAT64)          AS asofa_total_score,
    CAST(b.sofa_total_score  AS FLOAT64)          AS sofa_total_score,
    CAST(b.charlson_comorbidity_index AS FLOAT64) AS charlson_comorbidity_index,
    -- component flags (kept as-is)
    b.myocardial_infarct, b.congestive_heart_failure, b.peripheral_vascular_disease,
    b.cerebrovascular_disease, b.dementia, b.chronic_pulmonary_disease, b.rheumatic_disease,
    b.peptic_ulcer_disease, b.mild_liver_disease, b.severe_liver_disease,
    b.diabetes_without_cc, b.diabetes_with_cc, b.paraplegia, b.renal_disease,
    b.malignant_cancer, b.metastatic_solid_tumor, b.aids
  FROM `{PROJECT}.{MIMIC_DATASET}.mimic_baseline_characteristics` b
),
mort AS (
  SELECT subject_id, stay_id,
         CAST(mortality_30day AS INT64) AS mortality_30day
  FROM `{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total`
)
SELECT
  base.*, mort.mortality_30day
FROM base
LEFT JOIN mort USING (subject_id, stay_id)
")

sql_mimic_sep <- glue::glue("
WITH base AS (
  SELECT
    b.subject_id, b.stay_id,
    CAST(b.age AS FLOAT64)                        AS age,
    SAFE_CAST(b.gender AS INT64)                  AS gender,
    CAST(b.length_of_stay AS FLOAT64)             AS length_of_stay,
    CAST(b.asofa_total_score AS FLOAT64)          AS asofa_total_score,
    CAST(b.sofa_total_score  AS FLOAT64)          AS sofa_total_score,
    CAST(b.charlson_comorbidity_index AS FLOAT64) AS charlson_comorbidity_index,
    b.myocardial_infarct, b.congestive_heart_failure, b.peripheral_vascular_disease,
    b.cerebrovascular_disease, b.dementia, b.chronic_pulmonary_disease, b.rheumatic_disease,
    b.peptic_ulcer_disease, b.mild_liver_disease, b.severe_liver_disease,
    b.diabetes_without_cc, b.diabetes_with_cc, b.paraplegia, b.renal_disease,
    b.malignant_cancer, b.metastatic_solid_tumor, b.aids
  FROM `{PROJECT}.{MIMIC_DATASET}.mimic_baseline_characteristics_sepsis` b
),
mort AS (
  SELECT subject_id, stay_id,
         CAST(mortality_30day AS INT64) AS mortality_30day
  FROM `{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total_sepsis`
)
SELECT
  base.*, mort.mortality_30day
FROM base
LEFT JOIN mort USING (subject_id, stay_id)
")

sql_eicu_all <- glue::glue("
SELECT
  CAST(age AS FLOAT64)                           AS age,
  SAFE_CAST(gender AS INT64)                     AS gender,
  CAST(length_of_stay AS FLOAT64)                AS length_of_stay,
  CAST(mortality_30day AS INT64)                 AS mortality_30day,
  CAST(asofa_total_score AS FLOAT64)             AS asofa_total_score,
  CAST(sofa_total_score  AS FLOAT64)             AS sofa_total_score,
  CAST(charlson_comorbidity_index AS FLOAT64)    AS charlson_comorbidity_index,
  myocardial_infarct, congestive_heart_failure, peripheral_vascular_disease,
  cerebrovascular_disease, dementia, chronic_pulmonary_disease, rheumatic_disease,
  peptic_ulcer_disease, mild_liver_disease, severe_liver_disease,
  diabetes_without_cc, diabetes_with_cc, paraplegia, renal_disease,
  malignant_cancer, metastatic_solid_tumor, aids
FROM `{PROJECT}.{EICU_DATASET}.eicu_baseline_characteristics`
")

sql_eicu_sep <- glue::glue("
SELECT
  CAST(age AS FLOAT64)                           AS age,
  SAFE_CAST(gender AS INT64)                     AS gender,
  CAST(length_of_stay AS FLOAT64)                AS length_of_stay,
  CAST(mortality_30day AS INT64)                 AS mortality_30day,
  CAST(asofa_total_score AS FLOAT64)             AS asofa_total_score,
  CAST(sofa_total_score  AS FLOAT64)             AS sofa_total_score,
  CAST(charlson_comorbidity_index AS FLOAT64)    AS charlson_comorbidity_index,
  myocardial_infarct, congestive_heart_failure, peripheral_vascular_disease,
  cerebrovascular_disease, dementia, chronic_pulmonary_disease, rheumatic_disease,
  peptic_ulcer_disease, mild_liver_disease, severe_liver_disease,
  diabetes_without_cc, diabetes_with_cc, paraplegia, renal_disease,
  malignant_cancer, metastatic_solid_tumor, aids
FROM `{PROJECT}.{EICU_DATASET}.eicu_baseline_characteristics_sepsis`
")

m_all  <- DBI::dbGetQuery(con, sql_mimic_all)
e_all  <- DBI::dbGetQuery(con, sql_eicu_all)
m_sep  <- DBI::dbGetQuery(con, sql_mimic_sep)
e_sep  <- DBI::dbGetQuery(con, sql_eicu_sep)
DBI::dbDisconnect(con)

# ---- helpers (unchanged) ----
fmt_med_iqr <- function(x, integer_like = FALSE, digits = 2) {
  x <- suppressWarnings(as.numeric(x)); x <- x[is.finite(x)]
  if (!length(x)) return(NA_character_)
  q <- stats::quantile(x, c(.25,.5,.75), na.rm = TRUE, type = 2)
  if (integer_like) {
    sprintf("%d [%d, %d]", round(q[2]), round(q[1]), round(q[3]))
  } else {
    sprintf(paste0("%.", digits, "f [%.", digits, "f, %.", digits, "f]"), q[2], q[1], q[3])
  }
}
fmt_pct <- function(x01) {
  x01 <- suppressWarnings(as.integer(x01))
  x01 <- x01[x01 %in% c(0L,1L)]
  if (!length(x01)) return(NA_character_)
  sprintf("%.2f", mean(x01) * 100)
}
smd_cont <- function(x, y) {
  x <- suppressWarnings(as.numeric(x)); y <- suppressWarnings(as.numeric(y))
  x <- x[is.finite(x)]; y <- y[is.finite(y)]
  if (!length(x) || !length(y)) return(NA_real_)
  mx <- mean(x); my <- mean(y)
  sx <- stats::sd(x); sy <- stats::sd(y)
  nx <- length(x); ny <- length(y)
  sp <- sqrt(((nx - 1) * sx^2 + (ny - 1) * sy^2) / (nx + ny - 2))
  if (!is.finite(sp) || sp == 0) return(NA_real_)
  abs((mx - my) / sp)
}
smd_binary <- function(x, y) {
  x <- suppressWarnings(as.integer(x)); y <- suppressWarnings(as.integer(y))
  x <- x[x %in% c(0L,1L)]; y <- y[y %in% c(0L,1L)]
  if (!length(x) || !length(y)) return(NA_real_)
  p1 <- mean(x); p2 <- mean(y)
  p  <- mean(c(x, y))
  denom <- sqrt(p * (1 - p))
  if (!is.finite(denom) || denom == 0) return(NA_real_)
  abs((p1 - p2) / denom)
}

# ---- binary normalization >0→1 & male (unchanged) ----
cci_bins <- c(
  "myocardial_infarct","congestive_heart_failure","peripheral_vascular_disease",
  "cerebrovascular_disease","dementia","chronic_pulmonary_disease",
  "rheumatic_disease","peptic_ulcer_disease","mild_liver_disease",
  "severe_liver_disease","diabetes_without_cc","diabetes_with_cc",
  "paraplegia","renal_disease","malignant_cancer","metastatic_solid_tumor","aids"
)
to01 <- function(df, cols) {
  for (cc in cols) df[[cc]] <- as.integer(suppressWarnings(as.numeric(df[[cc]])) > 0)
  df
}
to_male01 <- function(g) {
  gg <- if (is.numeric(g)) g else if (is.character(g)) g else as.character(g)
  as.integer(as.character(gg) %in% c("1","M","Male","m","male"))
}
norm_df <- function(df) {
  df <- to01(df, cci_bins)
  df$male <- to_male01(df$gender)
  # alias sofa_total_score -> sofa_total for downstream consistency
  df$sofa_total <- suppressWarnings(as.numeric(df$sofa_total_score))
  df
}
m_all <- norm_df(m_all); e_all <- norm_df(e_all)
m_sep <- norm_df(m_sep); e_sep <- norm_df(e_sep)

# ---- variables spec (unchanged) ----
cont_vars <- tibble::tribble(
  ~label,                        ~col,                         ~int_like, ~digits,
  "Age",                         "age",                        TRUE,      NA,
  "Age-adjusted Charlson Index", "charlson_comorbidity_index", TRUE,      NA,
  "aSOFA",                       "asofa_total_score",          TRUE,      NA,
  "SOFA",                        "sofa_total",                 TRUE,      NA,
  "ICU Length of Stay",          "length_of_stay",             FALSE,     2
)
bin_vars <- tibble::tribble(
  ~label,                        ~col,
  "Male",                        "male",
  "Myocardial Infarct",          "myocardial_infarct",
  "Congestive Heart Failure",    "congestive_heart_failure",
  "Peripheral Vascular Disease", "peripheral_vascular_disease",
  "Cerebrovascular Disease",     "cerebrovascular_disease",
  "Dementia",                    "dementia",
  "Chronic Pulmonary Disease",   "chronic_pulmonary_disease",
  "Rheumatic Disease",           "rheumatic_disease",
  "Peptic Ulcer Disease",        "peptic_ulcer_disease",
  "Mild Liver Disease",          "mild_liver_disease",
  "Severe Liver Disease",        "severe_liver_disease",
  "Diabetes Without CC",         "diabetes_without_cc",
  "Diabetes With CC",            "diabetes_with_cc",
  "Paraplegia",                  "paraplegia",
  "Renal Disease",               "renal_disease",
  "Malignant Cancer",            "malignant_cancer",
  "Metastatic Solid Tumor",      "metastatic_solid_tumor",
  "AIDS",                        "aids"
)

# ---- builder for one row (cont/bin) (unchanged) ----
row_cont <- function(lbl, col, int_like, digits) {
  xm_all <- m_all[[col]]; xe_all <- e_all[[col]]
  xm_sep <- m_sep[[col]]; xe_sep <- e_sep[[col]]
  tibble::tibble(
    Variable = lbl,
    `MIMIC (All)` = if (isTRUE(int_like)) fmt_med_iqr(xm_all, TRUE) else fmt_med_iqr(xm_all, FALSE, ifelse(is.na(digits),2,digits)),
    `eICU (All)`  = if (isTRUE(int_like)) fmt_med_iqr(xe_all, TRUE) else fmt_med_iqr(xe_all, FALSE, ifelse(is.na(digits),2,digits)),
    `SMD (All)`   = {v <- smd_cont(xm_all, xe_all); ifelse(is.na(v),"—",sprintf("%.2f",v))},
    ` `           = "",  # spacer
    `MIMIC (Sepsis)` = if (isTRUE(int_like)) fmt_med_iqr(xm_sep, TRUE) else fmt_med_iqr(xm_sep, FALSE, ifelse(is.na(digits),2,digits)),
    `eICU (Sepsis)`  = if (isTRUE(int_like)) fmt_med_iqr(xe_sep, TRUE) else fmt_med_iqr(xe_sep, FALSE, ifelse(is.na(digits),2,digits)),
    `SMD (Sepsis)`   = {v <- smd_cont(xm_sep, xe_sep); ifelse(is.na(v),"—",sprintf("%.2f",v))}
  )
}
row_bin <- function(lbl, col) {
  xm_all <- m_all[[col]]; xe_all <- e_all[[col]]
  xm_sep <- m_sep[[col]]; xe_sep <- e_sep[[col]]
  tibble::tibble(
    Variable = lbl,
    `MIMIC (All)` = fmt_pct(xm_all),
    `eICU (All)`  = fmt_pct(xe_all),
    `SMD (All)`   = {v <- smd_binary(xm_all, xe_all); ifelse(is.na(v),"—",sprintf("%.2f",v))},
    ` `           = "",
    `MIMIC (Sepsis)` = fmt_pct(xm_sep),
    `eICU (Sepsis)`  = fmt_pct(xe_sep),
    `SMD (Sepsis)`   = {v <- smd_binary(xm_sep, xe_sep); ifelse(is.na(v),"—",sprintf("%.2f",v))}
  )
}

# ---- assemble rows (unchanged) ----
rows <- list()

# N row
rows[["N"]] <- tibble::tibble(
  Variable = "N",
  `MIMIC (All)` = format(nrow(m_all), big.mark = ","),
  `eICU (All)`  = format(nrow(e_all), big.mark = ","),
  `SMD (All)`   = "—",
  ` `           = "",
  `MIMIC (Sepsis)` = format(nrow(m_sep), big.mark = ","),
  `eICU (Sepsis)`  = format(nrow(e_sep), big.mark = ","),
  `SMD (Sepsis)`   = "—"
)

# continuous
for (i in seq_len(nrow(cont_vars))) {
  r <- row_cont(cont_vars$label[i], cont_vars$col[i], cont_vars$int_like[i], cont_vars$digits[i])
  rows[[cont_vars$label[i]]] <- r
}

# binary
for (i in seq_len(nrow(bin_vars))) {
  r <- row_bin(bin_vars$label[i], bin_vars$col[i])
  rows[[bin_vars$label[i]]] <- r
}

# mortality row (percent)
rows[["30-day In-hospital Mortality"]] <- row_bin("30-day In-hospital Mortality", "mortality_30day")

# order (unchanged)
order_vars <- c(
  "N",
  "Age","Male",
  "Myocardial Infarct","Congestive Heart Failure","Peripheral Vascular Disease",
  "Cerebrovascular Disease","Dementia","Chronic Pulmonary Disease",
  "Rheumatic Disease","Peptic Ulcer Disease","Mild Liver Disease",
  "Severe Liver Disease","Diabetes Without CC","Diabetes With CC",
  "Paraplegia","Renal Disease","Malignant Cancer","Metastatic Solid Tumor","AIDS",
  "Age-adjusted Charlson Index","aSOFA","SOFA",
  "ICU Length of Stay",
  "30-day In-hospital Mortality"
)

final_tbl <- purrr::map_dfr(order_vars, function(v) rows[[v]])

# % sign for binary (unchanged)
binary_labels <- c(bin_vars$label, "30-day In-hospital Mortality")
pct_cols <- c("MIMIC (All)","eICU (All)","MIMIC (Sepsis)","eICU (Sepsis)")
display_tbl <- final_tbl %>%
  dplyr::mutate(across(dplyr::all_of(pct_cols),
    ~ ifelse(Variable %in% binary_labels & !stringr::str_detect(.x,"\\["),
             paste0(.x, "%"), .x)))

# ---- save CSV ----
readr::write_csv(display_tbl, CSV_FILE)

# ---- gt render (layout preserved) ----
g <- gt::gt(display_tbl) %>%
  gt::tab_header(
    title = gt::md("**Baseline Characteristics (MIMIC vs eICU)**"),
    subtitle = "All ICU admissions vs Sepsis (first 24h); Continuous: median [IQR]; Binary: %"
  ) %>%
  gt::cols_label(
    Variable        = "Variable",
    `MIMIC (All)`   = "MIMIC-IV",
    `eICU (All)`    = "eICU",
    `SMD (All)`     = "SMD",
    ` `             = "",
    `MIMIC (Sepsis)`= "MIMIC-IV",
    `eICU (Sepsis)` = "eICU",
    `SMD (Sepsis)`  = "SMD"
  ) %>%
  gt::tab_spanner(label = "All ICU admissions", columns = c(`MIMIC (All)`,`eICU (All)`,`SMD (All)`)) %>%
  gt::tab_spanner(label = "Sepsis (first 24h or on arrival)", columns = c(`MIMIC (Sepsis)`,`eICU (Sepsis)`,`SMD (Sepsis)`)) %>%
  gt::cols_align("left",  columns = c(Variable)) %>%
  gt::cols_align("center",columns = c(`MIMIC (All)`,`eICU (All)`,`SMD (All)`,` `,
                                      `MIMIC (Sepsis)`,`eICU (Sepsis)`,`SMD (Sepsis)`)) %>%
  gt::opt_row_striping() %>%
  gt::tab_options(table.font.size = gt::px(12)) %>%
  gt::cols_width(` ` ~ gt::px(28))  # spacer

# Save PNG only (keep vwidth/vheight behavior)
gt::gtsave(g, PNG_FILE, vwidth = 1500, vheight = 0)
message("Saved CSV: ", CSV_FILE, "\nSaved PNG: ", PNG_FILE)
