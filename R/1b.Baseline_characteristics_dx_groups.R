# =====================================================================
# File: 1b.Baseline_characteristics_dx_groups.R
#
# What this script does (for the README header)
# ---------------------------------------------
# Builds a baseline-style table of ICU admission diagnosis groups comparing
# MIMIC-IV vs eICU-CRD using FIRST ICU stays only. For each dx group, it
# reports n (%) in each cohort and the standardized mean difference (SMD).
#
# How to use
# ----------
# 1) Edit the placeholders below:
#      USER_EMAIL    <- "YOUR_EMAIL_HERE"
#      PROJECT       <- "YOUR_PROJECT_ID"
#      MIMIC_DATASET <- "YOUR_MIMIC_DATASET"   # e.g., "mimic_output"
#      EICU_DATASET  <- "YOUR_EICU_DATASET"    # e.g., "eicu_output"
#
# 2) Ensure these tables exist (built from the provided SQL):
#      <PROJECT>.<MIMIC_DATASET>.mimic_first_icu_dx_groups
#      <PROJECT>.<EICU_DATASET>.eicu_first_icu_dx_groups
#    Required columns (both tables): dx_group, n_subjects, pct
#    (pct is % of the FIRST-ICU cohort; patients may belong to multiple groups.)
#
# Notes
# -----
# • Only email/project/dataset/table names & headers are adapted for reuse;
#   the table rendering (layout/width/height) follows the original 1b script.
# • SMD for binary proportions is |p1 − p2| / sqrt(p*(1−p)),
#   where p is the pooled proportion across cohorts.
# • Outputs: PNG + CSV to ~/Documents (no PDF/HTML).
# =====================================================================

suppressPackageStartupMessages({
  req <- c("DBI","bigrquery","dplyr","tidyr","readr","glue","gt","stringr","purrr","tibble")
  to_install <- req[!sapply(req, require, character.only = TRUE)]
  if (length(to_install)) install.packages(to_install, Ncpus = max(1, parallel::detectCores()-1))
  invisible(lapply(req, library, character.only = TRUE))
})

set.seed(123)

# ---- user-editable defaults ----
USER_EMAIL    <- "YOUR_EMAIL_HERE"
PROJECT       <- "YOUR_PROJECT_ID"
MIMIC_DATASET <- "YOUR_MIMIC_DATASET"  # e.g., "mimic_output"
EICU_DATASET  <- "YOUR_EICU_DATASET"   # e.g., "eicu_output"
# ---------------------------------

# ---- outputs (PNG & CSV only to ~/Documents) ----
OUT_DIR  <- file.path(path.expand("~"), "Documents")
PNG_FILE <- file.path(OUT_DIR, "dxgroups_minimal_table.png")
CSV_FILE <- file.path(OUT_DIR, "mimic_eicu_dxgroups_with_smd.csv")
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

# ---- auth & BigQuery connection ----
options(gargle_oauth_email = USER_EMAIL, gargle_oauth_cache = TRUE)
con <- DBI::dbConnect(
  bigrquery::bigquery(),
  project = PROJECT,
  billing = PROJECT,
  use_legacy_sql = FALSE
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE))

# ---- read two source tables (created by the SQL scripts) ----
mimic_tbl <- DBI::dbGetQuery(
  con,
  glue::glue("SELECT dx_group, n_subjects, pct FROM `{PROJECT}.{MIMIC_DATASET}.mimic_first_icu_dx_groups`")
)
eicu_tbl <- DBI::dbGetQuery(
  con,
  glue::glue("SELECT dx_group, n_subjects, pct FROM `{PROJECT}.{EICU_DATASET}.eicu_first_icu_dx_groups`")
)

# ---- helpers: derive cohort denominators robustly from (n, pct) ----
estimate_denominator <- function(n, pct) {
  n  <- suppressWarnings(as.numeric(n))
  pc <- suppressWarnings(as.numeric(pct))
  v  <- n / (pc / 100)              # candidates
  v  <- v[is.finite(v) & (pc > 0)]
  if (!length(v)) return(NA_integer_)
  cand <- round(v)
  if (!length(cand)) return(NA_integer_)
  tab <- sort(table(cand), decreasing = TRUE)
  as.integer(names(tab)[1])
}

N_mimic <- estimate_denominator(mimic_tbl$n_subjects, mimic_tbl$pct)
if (is.na(N_mimic)) {
  v <- mimic_tbl$n_subjects / (mimic_tbl$pct/100)
  v <- v[is.finite(v) & (mimic_tbl$pct > 0)]
  if (length(v)) N_mimic <- as.integer(round(median(v)))
}

N_eicu <- estimate_denominator(eicu_tbl$n_subjects, eicu_tbl$pct)
if (is.na(N_eicu)) {
  v <- eicu_tbl$n_subjects / (eicu_tbl$pct/100)
  v <- v[is.finite(v) & (eicu_tbl$pct > 0)]
  if (length(v)) N_eicu <- as.integer(round(median(v)))
}

# ---- helpers: formatters (vector-safe) & SMD (vector-safe) ----
fmt_n_pct <- function(n, N, digits = 2) {
  n <- suppressWarnings(as.numeric(n))
  N <- suppressWarnings(as.numeric(N))
  len <- max(length(n), length(N))
  n <- rep_len(n, len)
  N <- rep_len(N, len)
  out <- rep(NA_character_, len)
  ok  <- is.finite(n) & is.finite(N) & N > 0
  if (any(ok)) {
    pct <- sprintf(paste0("%.", digits, "f"), 100 * n[ok] / N[ok])
    out[ok] <- paste0(format(ifelse(is.na(n[ok]), 0, n[ok]), big.mark=","), " (", pct, "%)")
  }
  out
}

smd_binary_counts <- function(n1, N1, n2, N2) {
  n1 <- suppressWarnings(as.numeric(n1))
  N1 <- suppressWarnings(as.numeric(N1))
  n2 <- suppressWarnings(as.numeric(n2))
  N2 <- suppressWarnings(as.numeric(N2))
  len <- max(length(n1), length(N1), length(n2), length(N2))
  n1 <- rep_len(n1, len); N1 <- rep_len(N1, len)
  n2 <- rep_len(n2, len); N2 <- rep_len(N2, len)
  out <- rep(NA_real_, len)
  ok  <- is.finite(n1) & is.finite(N1) & is.finite(n2) & is.finite(N2) & (N1 > 0) & (N2 > 0)
  if (!any(ok)) return(out)
  p1 <- n1[ok] / N1[ok]; p2 <- n2[ok] / N2[ok]
  p  <- (n1[ok] + n2[ok]) / (N1[ok] + N2[ok])
  denom <- sqrt(p * (1 - p))
  good  <- is.finite(denom) & (denom > 0)
  res <- rep(NA_real_, sum(ok))
  res[good] <- abs((p1[good] - p2[good]) / denom[good])
  out[ok] <- res
  out
}

# ---- join MIMIC & eICU by dx_group; compute displays and SMD ----
df <- dplyr::full_join(
  mimic_tbl %>% dplyr::rename(n_m = n_subjects, pct_m = pct),
  eicu_tbl  %>% dplyr::rename(n_e = n_subjects, pct_e = pct),
  by = "dx_group"
) %>%
  # missing groups in one cohort → count=0로 간주
  dplyr::mutate(
    n_m = dplyr::coalesce(n_m, 0L),
    n_e = dplyr::coalesce(n_e, 0L)
  ) %>%
  dplyr::mutate(
    `MIMIC-IV` = fmt_n_pct(n_m, N_mimic),
    eICU       = fmt_n_pct(n_e, N_eicu),
    SMD_val    = smd_binary_counts(n_m, N_mimic, n_e, N_eicu)
  ) %>%
  dplyr::mutate(SMD = ifelse(is.na(SMD_val), "\u2014", sprintf("%.2f", SMD_val))) %>%
  # 알파벳 순 정렬 (dx_group 오름차순)
  dplyr::arrange(dx_group) %>%
  dplyr::transmute(
    Variable = dx_group,
    `MIMIC-IV`, eICU, SMD
  )

# ---- save CSV ----
readr::write_csv(df, CSV_FILE)

# ---- gt table (layout/width/height preserved from original 1b) ----
BASE_FAMILY <- "Helvetica"
g <- gt::gt(df) %>%
  gt::tab_header(
    title = gt::md("**ICU Admission Diagnosis Groups (MIMIC vs eICU)**"),
    subtitle = "Binary: n (%); SMD shown"
  ) %>%
  gt::cols_label(
    Variable  = "Variable",
    `MIMIC-IV`= "MIMIC-IV",
    eICU      = "eICU",
    SMD       = "SMD"
  ) %>%
  gt::cols_align("left",  columns = c(Variable)) %>%
  gt::cols_align("center",columns = c(`MIMIC-IV`, eICU, SMD)) %>%
  gt::opt_row_striping() %>%
  gt::tab_options(table.font.names = BASE_FAMILY, table.font.size = gt::px(12))

# Save PNG only (no HTML/PDF); height auto like original
gt::gtsave(g, PNG_FILE, vwidth = 1200, vheight = 0)
message("Saved CSV: ", CSV_FILE, "\nSaved PNG: ", PNG_FILE)
