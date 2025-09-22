# =====================================================================
# File: 3.Cox.R
#
# What this script does (for the README header)
# ---------------------------------------------
# This script estimates discrimination metrics for 30-day in-hospital
# mortality comparing aSOFA vs SOFA across four cohorts using unadjusted
# Cox proportional hazards models (time horizon: 30 days).
# Specifically, it:
#   • Builds one Cox PH per score (aSOFA total, SOFA total) per dataset,
#     reporting hazard ratios (HRs) and bootstrap 95% CIs (B = 1,000).
#   • Computes Harrell’s C-index with bootstrap CIs and ΔC (aSOFA − SOFA).
#   • Produces (i) a forest plot PNG of HRs and (ii) a gt-formatted
#     discrimination table (PNG via webshot2) plus a CSV.
#
# Four datasets/panels:
#   (1) MIMIC-IV Total cohort
#   (2) MIMIC-IV Sepsis (within 24h) cohort
#   (3) eICU-CRD Total cohort
#   (4) eICU-CRD Sepsis (within 24h) cohort
#
# How to use
# ----------
# 1) Edit the placeholders below:
#      USER_EMAIL    <- "YOUR_EMAIL_HERE"
#      PROJECT       <- "YOUR_PROJECT_ID"
#      MIMIC_DATASET <- "YOUR_MIMIC_DATASET"   # e.g., "mimic_output"
#      EICU_DATASET  <- "YOUR_EICU_DATASET"    # e.g., "eicu_output"
#
# 2) Ensure these tables exist (built from the provided SQL scripts):
#      <PROJECT>.<MIMIC_DATASET>.mimic_asofa_total
#      <PROJECT>.<MIMIC_DATASET>.mimic_asofa_total_sepsis
#      <PROJECT>.<EICU_DATASET>.eicu_asofa_total
#      <PROJECT>.<EICU_DATASET>.eicu_asofa_total_sepsis
#
# 3) Required columns used here:
#      mortality_30day, length_of_stay, asofa_total_score, sofa_total_score
#    (Exactly as defined in the SQL outputs.)
#
# Notes
# -----
# • Only the email/project/dataset/table names and column headers were changed
#   to match the released SQL outputs; all analytic logic is unchanged.
# =====================================================================

suppressPackageStartupMessages({
req <- c("DBI","bigrquery","dplyr","tidyr","gt","stringr",
         "survival","ggplot2","purrr","ragg","readr","tibble","forcats","glue")
  to_install <- req[!sapply(req, require, character.only = TRUE)]
  if (length(to_install)) install.packages(to_install, Ncpus = max(1, parallel::detectCores()-1))
  invisible(lapply(req, library, character.only = TRUE))
})

set.seed(123)

# =========================
# 0) Output / layout
# =========================
OUT_DIR <- "~/Documents"

BASE_FAMILY <- "Arial"
BASE_SIZE   <- 5             # visual size ≈ 7pt in Word
DPI         <- 300

CONTENT_WIDTH_IN <- 6.5
FOREST_H_IN      <- 3.6
TABLE_H_IN       <- 4.8

to_px <- function(inches) inches * DPI

FOREST_PNG <- file.path(OUT_DIR, "cox_hr_forest.png")
TBL_PNG    <- file.path(OUT_DIR, "discrimination_table.png")
CSV_FILE   <- file.path(OUT_DIR, "discrimination_results.csv")

# =========================
# 1) Load data (BigQuery)
# =========================

# ---- User-configurable defaults (edit these) ----
USER_EMAIL    <- "YOUR_EMAIL_HERE"
PROJECT       <- "YOUR_PROJECT_ID"
MIMIC_DATASET <- "YOUR_MIMIC_DATASET"  # e.g., "mimic_output"
EICU_DATASET  <- "YOUR_EICU_DATASET"   # e.g., "eicu_output"
# -------------------------------------------------

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
      CAST(length_of_stay    AS FLOAT64) AS los,
      CAST(asofa_total_score AS FLOAT64) AS asofa_total,
      CAST(sofa_total_score  AS FLOAT64) AS sofa_total
    FROM `{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total`
  "),
  "MIMIC-IV (Sepsis)" = glue::glue("
    SELECT
      CAST(mortality_30day   AS INT64)   AS y,
      CAST(length_of_stay    AS FLOAT64) AS los,
      CAST(asofa_total_score AS FLOAT64) AS asofa_total,
      CAST(sofa_total_score  AS FLOAT64) AS sofa_total
    FROM `{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total_sepsis`
  "),
  "eICU-CRD (All)" = glue::glue("
    SELECT
      CAST(mortality_30day   AS INT64)   AS y,
      CAST(length_of_stay    AS FLOAT64) AS los,
      CAST(asofa_total_score AS FLOAT64) AS asofa_total,
      CAST(sofa_total_score  AS FLOAT64) AS sofa_total
    FROM `{PROJECT}.{EICU_DATASET}.eicu_asofa_total`
  "),
  "eICU-CRD (Sepsis)" = glue::glue("
    SELECT
      CAST(mortality_30day   AS INT64)   AS y,
      CAST(length_of_stay    AS FLOAT64) AS los,
      CAST(asofa_total_score AS FLOAT64) AS asofa_total,
      CAST(sofa_total_score  AS FLOAT64) AS sofa_total
    FROM `{PROJECT}.{EICU_DATASET}.eicu_asofa_total_sepsis`
  ")
)

datasets <- purrr::imap(sqls, ~{
  DBI::dbGetQuery(con, .x) %>% dplyr::mutate(.ds_label = .y)
})
DBI::dbDisconnect(con)

# =========================
# 2) Helpers & summaries
# =========================
fmt <- function(x, k = 3) formatC(x, digits = k, format = "f", drop0trailing = TRUE)
eps_time <- 1e-6
B <- 1000  # bootstrap reps

prep_surv_df <- function(df) {
  df %>%
    dplyr::select(y, los, asofa_total, sofa_total) %>%
    tidyr::drop_na() %>%
    dplyr::mutate(
      time  = pmax(pmin(los, 30), eps_time),
      event = as.integer(y == 1)
    )
}

safe_coxph_hr <- function(df, score_col) {
  dd <- df %>% dplyr::select(time, event, score = dplyr::all_of(score_col))
  if (var(dd$score, na.rm = TRUE) == 0) return(NA_real_)
  n_ev <- sum(dd$event)
  if (n_ev < 2 || n_ev > (nrow(dd) - 2)) return(NA_real_)
  if (length(unique(dd$time)) < 2) return(NA_real_)
  out <- try(survival::coxph(Surv(time, event) ~ score, data = dd, ties = "efron"), silent = TRUE)
  if (inherits(out, "try-error")) return(NA_real_)
  hr <- unname(exp(coef(out)[["score"]]))
  if (!is.finite(hr)) return(NA_real_)
  hr
}

safe_cindex <- function(df, score_col) {
  dd <- df %>% dplyr::select(time, event, score = dplyr::all_of(score_col))
  if (var(dd$score, na.rm = TRUE) == 0) return(NA_real_)
  out <- try(survival::survConcordance(Surv(time, event) ~ score, data = dd), silent = TRUE)
  if (inherits(out, "try-error")) return(NA_real_)
  as.numeric(out$concordance)
}

bootstrap_metrics_safe <- function(df, B = 1000, seed = 2025, max_tries_factor = 5) {
  set.seed(seed)
  n <- nrow(df)
  max_tries <- B * max_tries_factor

  hr_asofa_pt <- safe_coxph_hr(df, "asofa_total")
  hr_sofa_pt  <- safe_coxph_hr(df, "sofa_total")
  c_asofa_pt  <- safe_cindex(df, "asofa_total")
  c_sofa_pt   <- safe_cindex(df, "sofa_total")
  dC_pt       <- if (all(is.finite(c(c_asofa_pt, c_sofa_pt)))) c_asofa_pt - c_sofa_pt else NA_real_

  hr_a_bs <- numeric(0); hr_s_bs <- numeric(0)
  c_a_bs  <- numeric(0); c_s_bs  <- numeric(0); dC_bs <- numeric(0)
  skipped <- list(no_event = 0, var0 = 0, time_bad = 0, cox_error = 0, cidx_error = 0)

  tries <- 0
  while (length(hr_a_bs) < B && tries < max_tries) {
    tries <- tries + 1
    idx <- sample.int(n, n, replace = TRUE)
    d_b <- df[idx, , drop = FALSE]

    if (sum(d_b$event) < 2 || sum(d_b$event) > (nrow(d_b) - 2)) { skipped$no_event <- skipped$no_event + 1; next }
    if (length(unique(d_b$time)) < 2) { skipped$time_bad <- skipped$time_bad + 1; next }
    if (var(d_b$asofa_total) == 0 || var(d_b$sofa_total) == 0) { skipped$var0 <- skipped$var0 + 1; next }

    hr_a <- safe_coxph_hr(d_b, "asofa_total")
    hr_s <- safe_coxph_hr(d_b, "sofa_total")
    c_a  <- safe_cindex(d_b, "asofa_total")
    c_s  <- safe_cindex(d_b, "sofa_total")

    if (!is.finite(hr_a) || !is.finite(hr_s)) { skipped$cox_error <- skipped$cox_error + 1; next }
    if (!is.finite(c_a)  || !is.finite(c_s))  { skipped$cidx_error <- skipped$cidx_error + 1; next }

    hr_a_bs <- c(hr_a_bs, hr_a)
    hr_s_bs <- c(hr_s_bs, hr_s)
    c_a_bs  <- c(c_a_bs,  c_a)
    c_s_bs  <- c(c_s_bs,  c_s)
    dC_bs   <- c(dC_bs,   c_a - c_s)
  }

  pct <- function(x) if (length(x)) unname(quantile(x, c(0.025, 0.975), type = 7)) else c(NA_real_, NA_real_)

  list(
    point = list(hr_a = hr_asofa_pt, hr_s = hr_sofa_pt,
                 c_a = c_asofa_pt,   c_s = c_sofa_pt,
                 dC  = dC_pt),
    ci = list(
      hr_a = pct(hr_a_bs), hr_s = pct(hr_s_bs),
      c_a  = pct(c_a_bs),  c_s  = pct(c_s_bs),
      dC   = pct(dC_bs)
    ),
    effective_B = length(hr_a_bs),
    skips = skipped
  )
}

summarize_dataset <- function(df_raw, ds_label, B = 1000) {
  df <- prep_surv_df(df_raw)
  bm <- bootstrap_metrics_safe(df, B = B)

  forest <- tibble::tibble(
    Dataset = ds_label,
    Model   = c("aSOFA (total)", "SOFA (total)"),
    HR      = c(bm$point$hr_a, bm$point$hr_s),
    HR_lo   = c(bm$ci$hr_a[1],  bm$ci$hr_s[1]),
    HR_hi   = c(bm$ci$hr_a[2],  bm$ci$hr_s[2])
  )

  tbl <- tibble::tibble(
    Dataset = ds_label,
    Model   = c("aSOFA (total)", "SOFA (total)"),
    `Cox HR (95% CI)` = c(
      sprintf("%s [%s, %s]", fmt(bm$point$hr_a,3), fmt(bm$ci$hr_a[1],3), fmt(bm$ci$hr_a[2],3)),
      sprintf("%s [%s, %s]", fmt(bm$point$hr_s,3), fmt(bm$ci$hr_s[1],3), fmt(bm$ci$hr_s[2],3))
    ),
    `C-index (95% CI)` = c(
      sprintf("%s [%s, %s]", fmt(bm$point$c_a,3), fmt(bm$ci$c_a[1],3), fmt(bm$ci$c_a[2],3)),
      sprintf("%s [%s, %s]", fmt(bm$point$c_s,3), fmt(bm$ci$c_s[1],3), fmt(bm$ci$c_s[2],3))
    ),
    `Δ C-index (aSOFA−SOFA) (95% CI)` = c(
      sprintf("%s [%s, %s]", fmt(bm$point$dC,3), fmt(bm$ci$dC[1],3), fmt(bm$ci$dC[2],3)),
      ""
    )
  )

  list(forest = forest, table = tbl)
}

order_levels <- c("MIMIC-IV (All)", "MIMIC-IV (Sepsis)", "eICU-CRD (All)", "eICU-CRD (Sepsis)")

summaries <- purrr::imap(datasets, ~ summarize_dataset(.x %>% dplyr::select(y, los, asofa_total, sofa_total),
                                                      ds_label = .y, B = B))

forest_df <- purrr::map_dfr(summaries, "forest") %>%
  dplyr::mutate(Dataset = forcats::fct_relevel(Dataset, !!!order_levels))

table_df  <- purrr::map_dfr(summaries, "table") %>%
  dplyr::mutate(Dataset = forcats::fct_relevel(Dataset, !!!order_levels)) %>%
  dplyr::arrange(Dataset, dplyr::desc(Model))

# =========================
# 3) Forest plot
# =========================
shape_map <- c("aSOFA (total)" = 16, "SOFA (total)" = 17)
color_map <- c("aSOFA (total)" = "#0072B2", "SOFA (total)" = "#E69F00")

p <- ggplot(forest_df, aes(x = Dataset, y = HR, color = Model, shape = Model)) +
  geom_hline(yintercept = 1, linetype = "dashed", linewidth = 0.6) +
  geom_pointrange(
    aes(ymin = HR_lo, ymax = HR_hi),
    position = position_dodge(width = 0.3),
    linewidth = 0.7, fatten = 2.0, show.legend = FALSE
  ) +
  geom_point(
    position = position_dodge(width = 0.3),
    size = 2.5, stroke = 0, show.legend = TRUE
  ) +
  scale_color_manual(values = color_map, drop = FALSE) +
  scale_shape_manual(values = shape_map, drop = FALSE) +
  scale_x_discrete(expand = expansion(add = c(0.05, 0.05))) +
  scale_y_log10() +
  guides(color = guide_legend(title = NULL, override.aes = list(size = 2.2)),
         shape = "none") +
  labs(
    title = "Cox Hazard Ratios for 30-day In-hospital Mortality",
    subtitle = "Unadjusted Cox PH (LOS 30 days); Bootstrap 95% CIs (B = 1,000)",
    y = "Hazard Ratio (log scale)", x = NULL
  ) +
  theme_minimal(base_family = BASE_FAMILY, base_size = BASE_SIZE) +
  theme(
    legend.position       = "top",
    legend.text           = element_text(size = BASE_SIZE + 1, face = "bold"),
    plot.title            = element_text(size = BASE_SIZE + 2, face = "bold"),
    plot.subtitle         = element_text(size = BASE_SIZE),
    axis.text.x           = element_text(size = BASE_SIZE + 1, face = "bold", margin = margin(t = 4)),
    axis.text.y           = element_text(size = BASE_SIZE),
    axis.title.y          = element_text(size = BASE_SIZE + 1, margin = margin(r = 4)),
    plot.margin           = margin(t = 6, r = 12, b = 6, l = 12)
  )

ggsave(
  filename = FOREST_PNG,
  plot     = p,
  width    = CONTENT_WIDTH_IN,
  height   = FOREST_H_IN,
  units    = "in",
  dpi      = DPI,
  bg       = "white"
)
message("Saved forest plot PNG: ", FOREST_PNG)

# =========================
# 4) Discrimination table
# =========================
tbl_show <- table_df %>%
  dplyr::select(Dataset, Model,
                `Cox HR (95% CI)`,
                `C-index (95% CI)`,
                `Δ C-index (aSOFA−SOFA) (95% CI)`)

TABLE_W_PX <- to_px(CONTENT_WIDTH_IN)

w_model  <- round(TABLE_W_PX * 0.14)
w_hr     <- round(TABLE_W_PX * 0.29)
w_cidx   <- round(TABLE_W_PX * 0.28)
w_dc     <- round(TABLE_W_PX * 0.29)

tbl_gt <-
  tbl_show |>
  dplyr::mutate(Dataset = as.character(Dataset)) |>
  gt::gt(groupname_col = "Dataset") |>
  gt::tab_header(
    title = gt::md("**Discrimination (aSOFA vs SOFA)**"),
    subtitle = gt::md("Unadjusted Cox PH & Harrell's C (LOS 30 days); Bootstrap 95% CIs (B = 1,000)")
  ) |>
  gt::cols_width(
    Model ~ gt::px(w_model),
    `Cox HR (95% CI)` ~ gt::px(w_hr),
    `C-index (95% CI)` ~ gt::px(w_cidx),
    `Δ C-index (aSOFA−SOFA) (95% CI)` ~ gt::px(w_dc)
  ) |>
  gt::cols_align(align = "center", columns = gt::everything()) |>
  gt::tab_options(
    table.font.names = BASE_FAMILY,
    table.font.size  = gt::px(28),
    table.width      = gt::px(TABLE_W_PX),
    heading.align    = "center",
    column_labels.font.weight = "bold",
    data_row.padding = gt::px(3),
    row_group.padding = gt::px(3)
  ) |>
  gt::opt_row_striping()
  
gt::gtsave(
  tbl_gt,
  filename = TBL_PNG,
  vwidth   = TABLE_W_PX,
  vheight  = to_px(TABLE_H_IN)
)

message("Saved table PNG: ", TBL_PNG)

# =========================
# 5) Save CSV
# =========================
readr::write_csv(tbl_show, CSV_FILE)
