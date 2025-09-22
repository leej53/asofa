# =====================================================================
# File: 6.Missingness_day1_side_by_side.R
#
# What this script does (for the README header)
# ---------------------------------------------
# Builds a single side-by-side table that summarizes first-24h
# missingness and treatment-timing artifacts that motivate aSOFA,
# using the precomputed BigQuery table:
#   <PROJECT>.<OUTPUT_DATASET>.missingness_day1_side_by_side
#
# Output columns (4):
#   SOFA Variable | MIMIC-IV: n (%) | eICU-CRD: n (%) | Handling (pre-specified)
#
# The first data row lists total N for each cohort to aid interpretation.
# All counts/percents are taken from the BigQuery table; percentages are
# re-computed in R and shown with two decimals.
#
# How to use
# ----------
# 1) Edit the placeholders below:
#      USER_EMAIL     <- "YOUR_EMAIL_HERE"
#      PROJECT        <- "YOUR_PROJECT_ID"
#      OUTPUT_DATASET <- "YOUR_OUTPUT_DATASET"   # where the summary table was created
#      OUT_DIR        <- "~/Documents"           # where PNG/CSV will be saved
#
# 2) Make sure the following BigQuery table already exists (via provided SQL):
#      <PROJECT>.<OUTPUT_DATASET>.missingness_day1_side_by_side
#
# 3) Run the script. It will save a PNG table and a CSV alongside it.
#
# Notes
# -----
# • We intentionally mirror the style used in the other public R scripts:
#   DBI + bigrquery + glue; outputs via gt::gtsave (PNG only).
# • “Handling (pre-specified)” briefly documents how our aSOFA analysis
#   treated each artifact, as registered in the Methods.
# • For the “Urine output” row, we note that benchmark SOFA used UO in
#   206 (0.3%) MIMIC stays; aSOFA did not use UO for scoring.
# =====================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(bigrquery)
  library(glue)
  library(dplyr)
  library(readr)
  library(gt)
  library(stringr)
  library(scales)
})

# ---- user-editable defaults ----
USER_EMAIL     <- "YOUR_EMAIL_HERE"
PROJECT        <- "YOUR_PROJECT_ID"
OUTPUT_DATASET <- "YOUR_OUTPUT_DATASET"  # e.g., "asofa_outputs"
OUT_DIR        <- file.path(path.expand("~"), "Documents")
# --------------------------------

# ---- outputs (PNG & CSV only) ----
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)
PNG_FILE <- file.path(OUT_DIR, "missingness_day1_side_by_side.png")
CSV_FILE <- file.path(OUT_DIR, "missingness_day1_side_by_side.csv")

# ---- constants for notes ----
# If these change in your analysis, update here.
MIMIC_UO_USED_N   <- 206L
MIMIC_UO_USED_PCT <- 0.3  # percent

# ---- BigQuery connection ----
options(gargle_oauth_email = USER_EMAIL, gargle_oauth_cache = TRUE)
con <- DBI::dbConnect(
  bigrquery::bigquery(),
  project = PROJECT,
  billing = PROJECT,
  use_legacy_sql = FALSE
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE))

# ---- pull the side-by-side table ----
sql <- glue::glue("
  SELECT metric_label, mimic_n, mimic_total, eicu_n, eicu_total
  FROM `{PROJECT}.{OUTPUT_DATASET}.missingness_day1_side_by_side`
  ORDER BY metric_label
")
df <- DBI::dbGetQuery(con, sql) %>%
  tibble::as_tibble()

# We also want the original metric order as created in SQL. If the table
# still has metric_id, prefer that; otherwise retain the natural order we got.
# (No error if the column doesn't exist.)
if ("metric_id" %in% names(df)) {
  df <- DBI::dbGetQuery(con, glue::glue("
    SELECT metric_label, mimic_n, mimic_total, eicu_n, eicu_total
    FROM `{PROJECT}.{OUTPUT_DATASET}.missingness_day1_side_by_side`
    ORDER BY metric_id
  ")) %>% tibble::as_tibble()
}

# ---- formatters ----
fmt_npct <- function(n, d) {
  n <- as.numeric(n)
  d <- as.numeric(d)
  pct <- ifelse(d > 0, 100 * n / d, NA_real_)
  paste0(scales::comma(n), " (", sprintf("%.2f", pct), "%)")
}

# ---- handling column text (pre-specified) ----
handling_map <- c(
  "CV: MAP missing (min over 0–24h)" =
    "No vasopressor exposure → CV SOFA = 0; otherwise scored by vaso intensity.",
  "CV: Vaso order present but NO non-null infusion rate" =
    "Order-only treated as no vasopressor exposure.",
  "CV: At first addition of 2nd/3rd vaso, 1st-agent rate decreased" =
    "Descriptive only.",
  "Resp: PF ratio missing (ABG with PaO2 & FiO2 in 0–24h)" =
    "Respiratory SOFA = 0.",
  "Resp: First ABG at/after respiratory support start (0–24h)" =
    "Descriptive only.",
  "Renal: Creatinine missing (first-day min & max both NULL)" =
    "Renal SOFA = 0.",
  "Renal: Urine output missing (no output events in 0–24h)" =
    "Urine output not used in aSOFA."
)

# ---- build table rows ----
m_total <- unique(df$mimic_total)
e_total <- unique(df$eicu_total)
if (length(m_total) != 1L || length(e_total) != 1L) {
  stop("Unexpected totals: mimic_total/eicu_total should be constant across rows.")
}
m_total <- m_total[1]; e_total <- e_total[1]

order_vec <- c(
  "CV: MAP missing (min over 0–24h)",
  "CV: Vaso order present but NO non-null infusion rate",
  "CV: At first addition of 2nd/3rd vaso, 1st-agent rate decreased",
  "Resp: PF ratio missing (ABG with PaO2 & FiO2 in 0–24h)",
  "Resp: First ABG at/after respiratory support start (0–24h)",
  "Renal: Creatinine missing (first-day min & max both NULL)",
  "Renal: Urine output missing (no output events in 0–24h)"
)

df <- df |>
  mutate(metric_label = factor(metric_label, levels = order_vec)) |>
  arrange(metric_label)

rows <- df %>%
  transmute(
    `SOFA Variable`       = metric_label,
    `MIMIC-IV: n (%)`     = fmt_npct(mimic_n, mimic_total),
    `eICU-CRD: n (%)`     = fmt_npct(eicu_n,  eicu_total),
    `Handling (pre-specified)` = handling_map[metric_label] %||% ""
  )

# insert the N row right after the header
n_row <- tibble::tibble(
  `SOFA Variable`           = "N (total)",
  `MIMIC-IV: n (%)`         = scales::comma(m_total),
  `eICU-CRD: n (%)`         = scales::comma(e_total),
  `Handling (pre-specified)`= ""
)
rows_out <- dplyr::bind_rows(n_row, rows)

# ---- gt styling (consistent with other public scripts) ----
BASE_FAMILY <- "Arial"
TBL_W <- 1625
TBL_H <- 420 + 44 * nrow(rows_out)

tbl_gt <-
  rows_out |>
  gt::gt() |>
  gt::tab_header(
    title = gt::md("**First-24h Missingness & Treatment-timing Artifacts**"),
    subtitle = gt::md("Side-by-side counts and percentages; aSOFA handling shown at right")
  ) |>
  gt::cols_label(
    `SOFA Variable`            = "SOFA Variable",
    `MIMIC-IV: n (%)`          = "MIMIC-IV: n (%)",
    `eICU-CRD: n (%)`          = "eICU-CRD: n (%)",
    `Handling (pre-specified)` = "Handling (pre-specified)"
  ) |>
  gt::tab_options(
    table.font.names          = BASE_FAMILY,
    table.font.size           = gt::px(20),
    column_labels.font.weight = "bold",
    column_labels.font.size   = gt::px(22),
    data_row.padding          = gt::px(6),
    row_group.padding         = gt::px(6),
    table.width               = gt::px(TBL_W)
  ) |>
  gt::cols_width(
    `SOFA Variable`            ~ gt::px(540),
    `MIMIC-IV: n (%)`          ~ gt::px(270),
    `eICU-CRD: n (%)`          ~ gt::px(270),
    `Handling (pre-specified)` ~ gt::px(545)
  ) |>
  gt::opt_row_striping()

# ---- save PNG & CSV ----
gt::gtsave(tbl_gt, filename = PNG_FILE, vwidth = TBL_W, vheight = TBL_H)
message("Saved table PNG: ", PNG_FILE)
readr::write_csv(rows_out, CSV_FILE)
message("Saved CSV: ", CSV_FILE)
