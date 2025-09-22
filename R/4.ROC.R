# =====================================================================
# File: 4.ROC.R
#
# What this script does (for the README header)
# ---------------------------------------------
# This script produces a 2-in-1 ROC report comparing aSOFA vs SOFA for
# 30-day in-hospital mortality across four cohorts. Specifically:
#   • Panel figure: ROC curves (aSOFA in blue; SOFA in red) for:
#       (A) MIMIC-IV (All)  (B) MIMIC-IV (Sepsis within 24h)
#       (C) eICU-CRD (All)  (D) eICU-CRD (Sepsis within 24h)
#   • Table: AUCs (95% DeLong CIs), ΔAUC (aSOFA − SOFA) with 95% bootstrap CI,
#     paired DeLong p-values, plus N and event counts.
#
# How to use
# ----------
# 1) Edit the placeholders below:
#      USER_EMAIL    <- "YOUR_EMAIL_HERE"
#      PROJECT       <- "YOUR_PROJECT_ID"
#      MIMIC_DATASET <- "YOUR_MIMIC_DATASET"   # e.g., "mimic_output"
#      EICU_DATASET  <- "YOUR_EICU_DATASET"    # e.g., "eicu_output"
#
# 2) Make sure the following BigQuery tables exist (from the provided SQL):
#      <PROJECT>.<MIMIC_DATASET>.mimic_asofa_total
#      <PROJECT>.<MIMIC_DATASET>.mimic_asofa_total_sepsis
#      <PROJECT>.<EICU_DATASET>.eicu_asofa_total
#      <PROJECT>.<EICU_DATASET>.eicu_asofa_total_sepsis
#
# 3) Required columns (queried below):
#      mortality_30day, asofa_total_score, sofa_total_score
#    (These names match the SQL outputs.)
#
# Notes
# -----
# • Only email/project/dataset/table names and column headers were changed
#   to match the released SQL; all analysis/plotting logic is unchanged.
# • ΔAUC 95% CI is computed by stratified paired bootstrap (cases/controls) with B=1000.
# • Output format: PNG only (no HTML/PDF artifacts).
# =====================================================================

suppressPackageStartupMessages({
  req <- c("DBI","bigrquery","dplyr","tidyr","gt","stringr",
           "ggplot2","purrr","ragg","readr","tibble","forcats","pROC","glue")
  to_install <- req[!sapply(req, require, character.only = TRUE)]
  if (length(to_install)) install.packages(to_install, Ncpus = max(1, parallel::detectCores()-1))
  invisible(lapply(req, library, character.only = TRUE))
})

set.seed(123)

# ==== knobs ====
BASE_FAMILY <- "Arial"

OUT_DIR  <- file.path(path.expand("~"), "Documents")
ROC_PNG  <- file.path(OUT_DIR, "roc_panel_asofa_vs_sofa.png")
TBL_PNG  <- file.path(OUT_DIR, "auc_delong_table.png")
CSV_FILE <- file.path(OUT_DIR, "auc_delong_results.csv")

# Default sizes tuned for letter-width figures
PNG_W <- 1800; PNG_H <- 1200    # (initial; panel figure is reset to square later)
TBL_W <- 1600; TBL_H <- 520     # table png size

# ΔAUC bootstrap controls (adjust B down if resource-limited)
BOOT_B    <- 1000
BOOT_SEED <- 2025

fmt_num <- function(x, k = 3) formatC(x, digits = k, format = "f", drop0trailing = TRUE)
fmt_p   <- function(p) ifelse(is.na(p), "", ifelse(p < 0.001, "<0.001", formatC(p, digits = 3, format = "f")))

# ---- User-configurable defaults (edit these) ----
USER_EMAIL    <- "YOUR_EMAIL_HERE"
PROJECT       <- "YOUR_PROJECT_ID"
MIMIC_DATASET <- "YOUR_MIMIC_DATASET"  # e.g., "mimic_output"
EICU_DATASET  <- "YOUR_EICU_DATASET"   # e.g., "eicu_output"
# -------------------------------------------------

# ---- BigQuery pull (4 datasets) ----
options(gargle_oauth_email = USER_EMAIL, gargle_oauth_cache = TRUE)
con <- DBI::dbConnect(bigrquery::bigquery(),
  project = PROJECT,
  billing = PROJECT,
  use_legacy_sql = FALSE
)

sqls <- list(
  "MIMIC-IV (All)" = glue::glue("
    SELECT
      CAST(mortality_30day   AS INT64)   AS y,
      CAST(asofa_total_score AS FLOAT64) AS asofa_total,
      CAST(sofa_total_score  AS FLOAT64) AS sofa_total
    FROM `{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total`
  "),
  "MIMIC-IV (Sepsis)" = glue::glue("
    SELECT
      CAST(mortality_30day   AS INT64)   AS y,
      CAST(asofa_total_score AS FLOAT64) AS asofa_total,
      CAST(sofa_total_score  AS FLOAT64) AS sofa_total
    FROM `{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total_sepsis`
  "),
  "eICU-CRD (All)" = glue::glue("
    SELECT
      CAST(mortality_30day   AS INT64)   AS y,
      CAST(asofa_total_score AS FLOAT64) AS asofa_total,
      CAST(sofa_total_score  AS FLOAT64) AS sofa_total
    FROM `{PROJECT}.{EICU_DATASET}.eicu_asofa_total`
  "),
  "eICU-CRD (Sepsis)" = glue::glue("
    SELECT
      CAST(mortality_30day   AS INT64)   AS y,
      CAST(asofa_total_score AS FLOAT64) AS asofa_total,
      CAST(sofa_total_score  AS FLOAT64) AS sofa_total
    FROM `{PROJECT}.{EICU_DATASET}.eicu_asofa_total_sepsis`
  ")
)

datasets <- purrr::imap(sqls, ~{
  DBI::dbGetQuery(con, .x) %>%
    dplyr::select(y, asofa_total, sofa_total) %>%
    tidyr::drop_na() %>%
    dplyr::mutate(.ds_label = .y)
})
DBI::dbDisconnect(con)

order_levels <- c("MIMIC-IV (All)", "MIMIC-IV (Sepsis)",
                  "eICU-CRD (All)", "eICU-CRD (Sepsis)")

# ---- ΔAUC bootstrap (stratified paired) ----
boot_delta_auc_ci <- function(y, a, s, B = 1000, seed = 2025) {
  set.seed(seed)
  pos <- which(y == 1); neg <- which(y == 0)
  n_pos <- length(pos); n_neg <- length(neg)
  if (n_pos < 2 || n_neg < 2) return(c(NA_real_, NA_real_))
  bs <- replicate(B, {
    i_pos <- sample(pos, n_pos, replace = TRUE)
    i_neg <- sample(neg, n_neg, replace = TRUE)
    i <- c(i_pos, i_neg)
    ra <- pROC::roc(y[i], a[i], levels = c(0,1), direction = "<", quiet = TRUE)
    rs <- pROC::roc(y[i], s[i], levels = c(0,1), direction = "<", quiet = TRUE)
    as.numeric(pROC::auc(ra) - pROC::auc(rs))
  })
  qs <- quantile(bs, c(0.025, 0.975), type = 7, na.rm = TRUE)
  c(lo = unname(qs[1]), hi = unname(qs[2]))
}

# ---- ROC/DeLong per-dataset (unchanged except ΔAUC CI formatting) ----
compute_roc_set <- function(df, ds_label) {
  y <- df$y
  asofa <- df$asofa_total
  sofa  <- df$sofa_total
  n_tot <- length(y); n_evt <- sum(y == 1, na.rm = TRUE)

  roc_a <- pROC::roc(response = y, predictor = asofa,
                     levels = c(0,1), direction = "<", quiet = TRUE)
  roc_s <- pROC::roc(response = y, predictor = sofa,
                     levels = c(0,1), direction = "<", quiet = TRUE)

  auc_a <- as.numeric(pROC::auc(roc_a))
  auc_s <- as.numeric(pROC::auc(roc_s))
  ci_a  <- pROC::ci.auc(roc_a, conf.level = 0.95, method = "delong")
  ci_s  <- pROC::ci.auc(roc_s, conf.level = 0.95, method = "delong")

  del   <- pROC::roc.test(roc_a, roc_s, method = "delong", paired = TRUE)
  dAUC  <- as.numeric(auc_a - auc_s)
  pval  <- del$p.value

  # NEW: Bootstrap CI for ΔAUC (paired, stratified by outcome)
  dauc_ci <- boot_delta_auc_ci(y, asofa, sofa, B = BOOT_B, seed = BOOT_SEED)

  # ROC points (for plotting)
  df_roc <- dplyr::bind_rows(
    tibble::tibble(fpr = 1 - roc_a$specificities, tpr = roc_a$sensitivities, Model = "aSOFA (total)"),
    tibble::tibble(fpr = 1 - roc_s$specificities, tpr = roc_s$sensitivities, Model = "SOFA (total)")
  ) %>% dplyr::mutate(Dataset = ds_label)

  # table rows (aSOFA row carries Δ and p-value)
  tbl <- tibble::tibble(
    Dataset = ds_label,
    Model   = c("aSOFA (total)", "SOFA (total)"),
    `AUC (95% CI)` = c(
      sprintf("%s [%s, %s]", fmt_num(auc_a,3), fmt_num(ci_a[1],3), fmt_num(ci_a[3],3)),
      sprintf("%s [%s, %s]", fmt_num(auc_s,3), fmt_num(ci_s[1],3), fmt_num(ci_s[3],3))
    ),
    `Δ AUC (aSOFA−SOFA)` = c(
      if (all(is.finite(dauc_ci))) {
        sprintf("%s [%s, %s]", fmt_num(dAUC,3), fmt_num(dauc_ci[1],3), fmt_num(dauc_ci[2],3))
      } else {
        fmt_num(dAUC,3)
      },
      ""
    ),
    `DeLong p-value`     = c(fmt_p(pval), ""),
    N = c(n_tot, n_tot),
    Events = c(n_evt, n_evt)
  )

  list(roc_df = df_roc, table = tbl)
}

res_list <- purrr::imap(datasets, ~ compute_roc_set(.x, .y))

roc_panel_df <- purrr::map_dfr(res_list, "roc_df") %>%
  dplyr::mutate(Dataset = forcats::fct_relevel(Dataset, !!!order_levels))

table_df <- purrr::map_dfr(res_list, "table") %>%
  dplyr::mutate(Dataset = forcats::fct_relevel(Dataset, !!!order_levels)) %>%
  dplyr::arrange(Dataset, dplyr::desc(Model)) %>%
  dplyr::select(Dataset, Model, `AUC (95% CI)`, `Δ AUC (aSOFA−SOFA)`, `DeLong p-value`, N, Events)

# ---- ROC panel (color-blind-friendly; solid vs dashed) ----
color_map    <- c("aSOFA (total)" = "#0072B2",   # blue (Okabe–Ito)
                  "SOFA (total)"  = "#E69F00")  # orange (Okabe–Ito)
linetype_map <- c("aSOFA (total)" = "solid",
                  "SOFA (total)"  = "dashed")

p_roc <- ggplot(roc_panel_df, aes(x = fpr, y = tpr, color = Model, linetype = Model, group = Model)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", linewidth = 0.6) +
  geom_line(linewidth = 1.2) +
  scale_color_manual(values = color_map, drop = FALSE) +
  scale_linetype_manual(values = linetype_map, drop = FALSE) +
  scale_x_continuous(limits = c(0, 1),
                     breaks = seq(0, 1, 0.25),
                     expand = expansion(add = c(0.02, 0.02)),
                     minor_breaks = NULL) +
  scale_y_continuous(limits = c(0, 1),
                     breaks = seq(0, 1, 0.25),
                     expand = expansion(add = c(0.02, 0.02)),
                     minor_breaks = NULL) +
  facet_wrap(~ Dataset, ncol = 2) +
  labs(
    title = "ROC Curves: aSOFA vs SOFA (30-day In-hospital Mortality)",
    subtitle = "AUC by pROC; DeLong paired test per dataset; ΔAUC with bootstrap 95% CI",
    x = "False Positive Rate (1 − Specificity)", y = "True Positive Rate (Sensitivity)"
  ) +
  theme_minimal(base_family = BASE_FAMILY) +
  theme(
    legend.position   = "top",
    legend.title      = element_blank(),
    legend.text       = element_text(size = 14, face = "bold"),
    plot.title        = element_text(size = 20, face = "bold"),
    plot.subtitle     = element_text(size = 12),
    strip.text        = element_text(size = 14, face = "bold"),
    axis.text         = element_text(size = 12),
    axis.title        = element_text(size = 13),
    panel.grid.minor  = element_blank(),
    panel.spacing.x   = unit(28, "pt"),
    panel.spacing.y   = unit(22, "pt"),
    plot.margin       = margin(t = 8, r = 12, b = 8, l = 12),
    aspect.ratio      = 1
  )

# Save ROC panel PNG (square)
PNG_W <- 1600; PNG_H <- 1600
ragg::agg_png(ROC_PNG, width = PNG_W, height = PNG_H, units = "px", res = 130)
print(p_roc); dev.off()
message("Saved ROC panel PNG: ", ROC_PNG)

# ---- AUC/DeLong summary table (PNG only) ----
# Letter-friendly width
TBL_W <- 1100
TBL_H <- 420

tbl_gt <-
  table_df |>
  dplyr::mutate(Dataset = as.character(Dataset)) |>
  gt::gt(groupname_col = "Dataset") |>
  gt::tab_header(
    title = gt::md("**AUC Comparison (aSOFA vs SOFA)**"),
    subtitle = gt::md("AUCs with DeLong 95% CIs; ΔAUC (bootstrap 95% CI) and paired DeLong test per dataset")
  ) |>
  gt::cols_label(
    Model = "Model",
    `AUC (95% CI)` = "AUC (95% CI)",
    `Δ AUC (aSOFA−SOFA)` = "Δ AUC (aSOFA−SOFA)",
    `DeLong p-value` = "DeLong p-value",
    N = "N",
    Events = "Events"
  ) |>
  gt::cols_width(
    Model ~ gt::px(110),
    `AUC (95% CI)` ~ gt::px(190),
    `Δ AUC (aSOFA−SOFA)` ~ gt::px(200),
    `DeLong p-value` ~ gt::px(125),
    N ~ gt::px(70),
    Events ~ gt::px(70)
  ) |>
  gt::cols_align("center", columns = gt::everything()) |>
  gt::tab_options(
    table.font.names = BASE_FAMILY,
    table.font.size  = gt::px(16),
    heading.align    = "center",
    column_labels.font.weight = "bold",
    column_labels.font.size   = gt::px(16),
    data_row.padding = gt::px(2),
    row_group.padding = gt::px(2),
    table.width      = gt::px(TBL_W)
  ) |>
  gt::opt_row_striping()

# Save table PNG directly (no HTML/webshot)
gt::gtsave(tbl_gt, filename = TBL_PNG, vwidth = TBL_W, vheight = TBL_H)
message("Saved table PNG: ", TBL_PNG)

# CSV export
readr::write_csv(table_df, CSV_FILE)
message("Saved CSV: ", CSV_FILE)
