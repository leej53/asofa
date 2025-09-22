# =====================================================================
# File: 5.Discrepancy.R
#
# What this script does (for the README header)
# ---------------------------------------------
# This script reports discrepancy/agreeement between aSOFA and SOFA.
# Panel A: Bland–Altman plots for total scores (aSOFA − SOFA) across four
#          cohorts: (A) MIMIC-IV (All), (B) MIMIC-IV (Sepsis within 24h),
#          (C) eICU-CRD (All), (D) eICU-CRD (Sepsis within 24h).
# Panel B: Quadratic-weighted kappa for subscores (CV, Respiratory, Renal),
#          with 95% CIs, plus exact agreement and within ±1-point agreement.
#
# How to use
# ----------
# 1) Edit the placeholders below:
#      USER_EMAIL    <- "YOUR_EMAIL_HERE"
#      PROJECT       <- "YOUR_PROJECT_ID"
#      MIMIC_DATASET <- "YOUR_MIMIC_DATASET"   # e.g., "mimic_output"
#      EICU_DATASET  <- "YOUR_EICU_DATASET"    # e.g., "eicu_output"
#
# 2) Make sure these BigQuery tables already exist (from the provided SQL):
#      <PROJECT>.<MIMIC_DATASET>.mimic_asofa_total
#      <PROJECT>.<MIMIC_DATASET>.mimic_asofa_total_sepsis
#      <PROJECT>.<EICU_DATASET>.eicu_asofa_total
#      <PROJECT>.<EICU_DATASET>.eicu_asofa_total_sepsis
#
# 3) Required columns (queried below) include:
#      asofa_total_score, sofa_total_score, mortality_30day, length_of_stay,
#      asofa_cv_score,   sofa_cv_score,
#      asofa_resp_score, sofa_resp_score,
#      asofa_renal_score, sofa_renal_score
#    (These names mirror the released SQL outputs.)
#
# Notes
# -----
# • Only the email/project/dataset/table names and column headers were changed
#   to match the released SQL; all analysis/plotting logic remains unchanged.
# • If the kappa 95% CI is not available from the asymptotic method, a
#   stratified bootstrap CI (B = 1,000) is used as a fallback.
# • Outputs are PNG files only (no HTML/PDF artifacts).
# =====================================================================

## ---- CRAN Mirror & Download Method ----
options(
  repos = c(CRAN = "https://cloud.r-project.org"),
  download.file.method = "libcurl"
)

## ---- Packages: install iff needed, then load ----
suppressPackageStartupMessages({
  req <- c("DBI","bigrquery","dplyr","tidyr","gt","stringr",
           "ggplot2","purrr","ragg","readr","tibble",
           "forcats","data.table","DescTools","glue")
  to_install <- req[!sapply(req, require, character.only = TRUE)]
  if (length(to_install)) install.packages(to_install, Ncpus = max(1, parallel::detectCores()-1))
  invisible(lapply(req, library, character.only = TRUE))
})

set.seed(123)

# ==== knobs ====
BASE_FAMILY <- "Arial"
options(scipen = 999)

OUT_DIR <- file.path(path.expand("~"), "Documents")

# --- Bland–Altman (total) outputs ---
BA_PNG   <- file.path(OUT_DIR, "ba_panel_asofa_vs_sofa.png")
BA_TBL   <- file.path(OUT_DIR, "ba_summary_table.png")
BA_CSV   <- file.path(OUT_DIR, "ba_summary_results.csv")

# --- Weighted kappa (subscores) outputs ---
KAPPA_TBL_PNG <- file.path(OUT_DIR, "kappa_weighted_table.png")
KAPPA_CSV     <- file.path(OUT_DIR, "kappa_weighted_results.csv")

# Image/Table sizes
BA_PNG_W <- 1600; BA_PNG_H <- 1600
TBL_W    <- 1100; TBL_H_BA <- 420; TBL_H_KAPPA <- 520

# Bootstrap for kappa CI if NA  (UPDATED)
KAPPA_BOOTSTRAP_IF_NA <- TRUE     # enable fallback
KAPPA_BOOT_B          <- 1000     # ATS-friendly default; lower to 500 for quick dry-runs

fmt_num <- function(x, k = 3) formatC(x, digits = k, format = "f", drop0trailing = TRUE)
fmt_pct <- function(x, k = 1) paste0(formatC(100*x, digits = k, format = "f", drop0trailing = TRUE), "%")

# ---- User-configurable defaults (edit these) ----
USER_EMAIL    <- "YOUR_EMAIL_HERE"
PROJECT       <- "YOUR_PROJECT_ID"
MIMIC_DATASET <- "YOUR_MIMIC_DATASET"  # e.g., "mimic_output"
EICU_DATASET  <- "YOUR_EICU_DATASET"   # e.g., "eicu_output"
# -------------------------------------------------

# ---- BigQuery pull (4 datasets; column names standardized) ----
options(gargle_oauth_email = USER_EMAIL, gargle_oauth_cache = TRUE)
con <- DBI::dbConnect(bigrquery::bigquery(),
  project = PROJECT,
  billing = PROJECT,
  use_legacy_sql = FALSE
)

sqls <- list(
  "MIMIC-IV (All)" = glue::glue("
    SELECT
      CAST(mortality_30day AS INT64)      AS y,
      CAST(length_of_stay  AS FLOAT64)    AS los,
      CAST(asofa_total_score AS FLOAT64)  AS asofa_total_score,
      CAST(sofa_total_score  AS FLOAT64)  AS sofa_total_score,
      CAST(asofa_cv_score    AS INT64)    AS asofa_cv_score,
      CAST(sofa_cv_score     AS INT64)    AS sofa_cv_score,
      CAST(asofa_resp_score  AS INT64)    AS asofa_resp_score,
      CAST(sofa_resp_score   AS INT64)    AS sofa_resp_score,
      CAST(asofa_renal_score AS INT64)    AS asofa_renal_score,
      CAST(sofa_renal_score  AS INT64)    AS sofa_renal_score
    FROM `{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total`
  "),
  "MIMIC-IV (Sepsis)" = glue::glue("
    SELECT
      CAST(mortality_30day AS INT64)      AS y,
      CAST(length_of_stay  AS FLOAT64)    AS los,
      CAST(asofa_total_score AS FLOAT64)  AS asofa_total_score,
      CAST(sofa_total_score  AS FLOAT64)  AS sofa_total_score,
      CAST(asofa_cv_score    AS INT64)    AS asofa_cv_score,
      CAST(sofa_cv_score     AS INT64)    AS sofa_cv_score,
      CAST(asofa_resp_score  AS INT64)    AS asofa_resp_score,
      CAST(sofa_resp_score   AS INT64)    AS sofa_resp_score,
      CAST(asofa_renal_score AS INT64)    AS asofa_renal_score,
      CAST(sofa_renal_score  AS INT64)    AS sofa_renal_score
    FROM `{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total_sepsis`
  "),
  "eICU-CRD (All)" = glue::glue("
    SELECT
      CAST(mortality_30day AS INT64)      AS y,
      CAST(length_of_stay  AS FLOAT64)    AS los,
      CAST(asofa_total_score AS FLOAT64)  AS asofa_total_score,
      CAST(sofa_total_score  AS FLOAT64)  AS sofa_total_score,
      CAST(asofa_cv_score    AS INT64)    AS asofa_cv_score,
      CAST(sofa_cv_score     AS INT64)    AS sofa_cv_score,
      CAST(asofa_resp_score  AS INT64)    AS asofa_resp_score,
      CAST(sofa_resp_score   AS INT64)    AS sofa_resp_score,
      CAST(asofa_renal_score AS INT64)    AS asofa_renal_score,
      CAST(sofa_renal_score  AS INT64)    AS sofa_renal_score
    FROM `{PROJECT}.{EICU_DATASET}.eicu_asofa_total`
  "),
  "eICU-CRD (Sepsis)" = glue::glue("
    SELECT
      CAST(mortality_30day AS INT64)      AS y,
      CAST(length_of_stay  AS FLOAT64)    AS los,
      CAST(asofa_total_score AS FLOAT64)  AS asofa_total_score,
      CAST(sofa_total_score  AS FLOAT64)  AS sofa_total_score,
      CAST(asofa_cv_score    AS INT64)    AS asofa_cv_score,
      CAST(sofa_cv_score     AS INT64)    AS sofa_cv_score,
      CAST(asofa_resp_score  AS INT64)    AS asofa_resp_score,
      CAST(sofa_resp_score   AS INT64)    AS sofa_resp_score,
      CAST(asofa_renal_score AS INT64)    AS asofa_renal_score,
      CAST(sofa_renal_score  AS INT64)    AS sofa_renal_score
    FROM `{PROJECT}.{EICU_DATASET}.eicu_asofa_total_sepsis`
  ")
)

datasets <- purrr::imap(sqls, ~{
  DBI::dbGetQuery(con, .x) %>%
    # rename to short names used downstream (ba_compute, kappa_compute)
    dplyr::transmute(
      y, los,
      asofa_total = asofa_total_score,
      sofa_total  = sofa_total_score,
      asofa_cv    = asofa_cv_score,
      sofa_cv     = sofa_cv_score,
      asofa_resp  = asofa_resp_score,
      sofa_resp   = sofa_resp_score,
      asofa_renal = asofa_renal_score,
      sofa_renal  = sofa_renal_score
    ) %>%
    tidyr::drop_na(asofa_total, sofa_total) %>%
    dplyr::mutate(.ds_label = .y)
})
DBI::dbDisconnect(con)

order_levels <- c("MIMIC-IV (All)", "MIMIC-IV (Sepsis)",
                  "eICU-CRD (All)", "eICU-CRD (Sepsis)")

# ---- Bland–Altman helpers (unchanged) ----
ba_compute <- function(df, a_col = "asofa_total", s_col = "sofa_total", log_ratio = FALSE) {
  DT <- data.table::as.data.table(df)[, .(a = get(a_col), s = get(s_col))]
  if (log_ratio) {
    DT <- DT[!is.na(a) & !is.na(s) & a > 0 & s > 0]
    DT[, `:=`(avg = (log(a) + log(s))/2, diff = log(a) - log(s))]
    ylab <- "Difference (log aSOFA − log SOFA)"; xlab <- "Mean of logs"
  } else {
    DT <- DT[!is.na(a) & !is.na(s)]
    DT[, `:=`(avg = (a + s)/2, diff = a - s)]
    ylab <- "Difference (aSOFA − SOFA)"; xlab <- "Mean of scores"
  }
  n   <- nrow(DT)
  dbar <- DT[, mean(diff)]
  s    <- DT[, sd(diff)]
  loa_low  <- dbar - 1.96*s
  loa_high <- dbar + 1.96*s

  fit <- lm(diff ~ avg, data = DT)   # proportional bias
  slope <- coef(fit)[2]; p_slope <- summary(fit)$coefficients[2,4]

  list(n = n, bias = dbar, sd = s, loa_low = loa_low, loa_high = loa_high,
       slope = as.numeric(slope), p_slope = as.numeric(p_slope),
       df_plot = as.data.frame(DT), ylab = ylab, xlab = xlab)
}

# ---- (fallback) bootstrap CI for kappa if NA ----
kappa_boot_ci <- function(tab, B = 500, conf = 0.95, seed = 123) {
  set.seed(seed)
  n <- sum(tab); p <- as.vector(tab) / n
  samp <- replicate(B, {
    cnt <- as.vector(rmultinom(1, n, prob = p))
    dim(cnt) <- dim(tab)
    ck <- DescTools::CohenKappa(cnt, weights = "Fleiss-Cohen")
    if (is.list(ck)) as.numeric(ck$kappa) else as.numeric(ck)
  })
  c(lower = unname(quantile(samp, (1-conf)/2, names = FALSE)),
    upper = unname(quantile(samp, 1-(1-conf)/2, names = FALSE)))
}

# ---- Weighted kappa helper (quadratic/Fleiss–Cohen) ----
kappa_compute <- function(df, a_col, s_col, levels = 0:4, bootstrap_if_na = FALSE) {
  DT <- data.table::as.data.table(df)[, .(a = as.integer(get(a_col)), s = as.integer(get(s_col)))]
  DT <- DT[!is.na(a) & !is.na(s)]
  a_f <- factor(pmax(pmin(DT$a, max(levels)), min(levels)), levels = levels)
  s_f <- factor(pmax(pmin(DT$s, max(levels)), min(levels)), levels = levels)
  tab <- table(a_f, s_f)

  ck <- DescTools::CohenKappa(tab, weights = "Fleiss-Cohen", conf.level = 0.95)

  if (is.list(ck)) {
    kappa  <- unname(ck$kappa)
    ci     <- unname(ck$conf.int)
    ci_lo  <- as.numeric(ci[1]); ci_hi <- as.numeric(ci[2])
  } else {
    kappa <- as.numeric(ck)
    ci_attr <- attr(ck, "conf.int")
    if (is.null(ci_attr)) { ci_lo <- NA_real_; ci_hi <- NA_real_ }
    else { ci_lo <- as.numeric(ci_attr[1]); ci_hi <- as.numeric(ci_attr[2]) }
  }

  # UPDATED: use bootstrap fallback with B = KAPPA_BOOT_B when CI is NA
  if (bootstrap_if_na && (is.na(ci_lo) || is.na(ci_hi))) {
    ci_bs <- kappa_boot_ci(tab, B = KAPPA_BOOT_B, conf = 0.95)
    ci_lo <- ci_bs["lower"]; ci_hi <- ci_bs["upper"]
  }

  exact   <- sum(diag(tab)) / sum(tab)
  within1 <- sum(tab[abs(row(tab) - col(tab)) <= 1]) / sum(tab)  # |diff| <= 1

  list(kappa = kappa, ci_lo = ci_lo, ci_hi = ci_hi,
       exact = exact, within1 = within1, n = sum(tab))
}

# =========================
# 1) Bland–Altman (TOTAL)
# =========================
ba_list <- purrr::imap(datasets, ~{
  out <- ba_compute(.x, "asofa_total", "sofa_total", log_ratio = FALSE)
  out$label <- .y
  out
})

# BA panel (2x2)
ba_panel_df <- purrr::map_dfr(ba_list, ~{
  .x$df_plot %>%
    dplyr::mutate(Dataset = .x$label)
}) %>%
  dplyr::mutate(Dataset = forcats::fct_relevel(
    Dataset,
    "MIMIC-IV (All)",
    "MIMIC-IV (Sepsis)",
    "eICU-CRD (All)",
    "eICU-CRD (Sepsis)"
  ))

ba_summ_tbl <- purrr::map_dfr(ba_list, ~{
  tibble::tibble(
    Dataset = .x$label,
    N       = .x$n,
    Bias    = fmt_num(.x$bias, 2),
    SD      = fmt_num(.x$sd, 2),
    `LoA (low, high)` = sprintf("[%s, %s]", fmt_num(.x$loa_low, 2), fmt_num(.x$loa_high, 2)),
    `Proportional bias slope (p)` = sprintf("%s (p=%s)", fmt_num(.x$slope, 3),
                                            ifelse(.x$p_slope < 0.001, "<0.001", fmt_num(.x$p_slope, 3)))
  )
}) %>%
  dplyr::mutate(Dataset = forcats::fct_relevel(Dataset, !!!order_levels)) %>%
  dplyr::arrange(Dataset)

p_ba <- ggplot(ba_panel_df, aes(x = (a + s)/2, y = (a - s))) +
  geom_bin2d(bins = 60) +
  geom_hline(yintercept = 0, linetype = "dotted", linewidth = 0.6) +
  facet_wrap(~ Dataset, ncol = 2) +
  labs(
    title = "Bland–Altman: aSOFA vs SOFA (Total Score)",
    subtitle = "Bias and 95% Limits of Agreement (per dataset); bin2d for density",
    x = "Mean of scores", y = "Difference (aSOFA − SOFA)"
  ) +
  theme_minimal(base_family = BASE_FAMILY) +
  theme(
    legend.position  = "none",
    plot.title       = element_text(size = 20, face = "bold"),
    plot.subtitle    = element_text(size = 12),
    strip.text       = element_text(size = 14, face = "bold"),
    axis.text        = element_text(size = 12),
    axis.title       = element_text(size = 13),
    panel.grid.minor = element_blank(),
    panel.spacing.x  = unit(28, "pt"),
    panel.spacing.y  = unit(22, "pt"),
    plot.margin      = margin(t = 8, r = 12, b = 8, l = 12),
    aspect.ratio     = 1
  )

ragg::agg_png(BA_PNG, width = BA_PNG_W, height = BA_PNG_H, units = "px", res = 130)
print(p_ba); dev.off()
message("Saved BA panel PNG: ", BA_PNG)

# BA summary table (PNG only)
ba_gt <-
  ba_summ_tbl |>
  gt::gt() |>
  gt::tab_header(
    title = gt::md("**Bland–Altman Summary: aSOFA vs SOFA (Total)**"),
    subtitle = gt::md("Bias, SD of differences, and 95% Limits of Agreement per dataset")
  ) |>
  gt::cols_label(
    Dataset = "Dataset", N = "N", Bias = "Bias", SD = "SD",
    `LoA (low, high)` = "95% LoA", `Proportional bias slope (p)` = "Proportional bias (slope, p)"
  ) |>
  gt::cols_width(
    Dataset ~ gt::px(200),
    N ~ gt::px(80),
    Bias ~ gt::px(80),
    SD ~ gt::px(80),
    `LoA (low, high)` ~ gt::px(200),
    `Proportional bias slope (p)` ~ gt::px(220)
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

gt::gtsave(ba_gt, filename = BA_TBL, vwidth = TBL_W, vheight = TBL_H_BA)
message("Saved BA table PNG: ", BA_TBL)

readr::write_csv(ba_summ_tbl %>% dplyr::mutate(Dataset = as.character(Dataset)), BA_CSV)
message("Saved BA CSV: ", BA_CSV)

# ==============================
# 2) Weighted Kappa (SUBSCORES)
# ==============================
subscores <- c("CV" = "cv", "Resp" = "resp", "Renal" = "renal")

kappa_rows <- list()
for (ds_name in names(datasets)) {
  df <- datasets[[ds_name]]
  for (nm in names(subscores)) {
    key   <- subscores[[nm]]
    a_col <- paste0("asofa_", key)
    s_col <- paste0("sofa_",  key)
    kap <- kappa_compute(df, a_col, s_col, levels = 0:4,
                         bootstrap_if_na = KAPPA_BOOTSTRAP_IF_NA)
    kappa_rows[[length(kappa_rows)+1]] <- tibble::tibble(
      Dataset = ds_name,
      Subscore = nm,
      `Weighted κ (95% CI)` = sprintf("%s [%s, %s]",
        fmt_num(kap$kappa, 3), fmt_num(kap$ci_lo, 3), fmt_num(kap$ci_hi, 3)),
      `Exact agreement` = fmt_pct(kap$exact, 1),
      `Within ±1 point` = fmt_pct(kap$within1, 1),
      N = kap$n
    )
  }
}

kappa_tbl <- dplyr::bind_rows(kappa_rows) %>%
  dplyr::group_by(Dataset, Subscore) %>% dplyr::slice_head(n = 1) %>% dplyr::ungroup() %>%
  dplyr::mutate(
    Dataset  = forcats::fct_relevel(Dataset, !!!order_levels),
    Subscore = factor(Subscore, levels = c("CV","Resp","Renal"))
  ) %>%
  dplyr::arrange(Dataset, Subscore)

if (nrow(kappa_tbl)) {
  kappa_gt <-
    kappa_tbl |>
    gt::gt(groupname_col = "Dataset") |>
    gt::tab_header(
      title = gt::md("**Agreement (Quadratic-weighted Kappa) for Subscores**"),
      subtitle = gt::md("SOFA vs aSOFA for CV, Respiratory, and Renal subscores (0–4)")
    ) |>
    gt::cols_label(
      Subscore = "Subscore",
      `Weighted κ (95% CI)` = "Weighted κ (95% CI)",
      `Exact agreement`     = "Exact agreement",
      `Within ±1 point`     = "Within ±1 point",
      N = "N"
    ) |>
    gt::cols_width(
      Subscore ~ gt::px(90),
      `Weighted κ (95% CI)` ~ gt::px(190),
      `Exact agreement`     ~ gt::px(120),
      `Within ±1 point`     ~ gt::px(120),
      N ~ gt::px(70)
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

  gt::gtsave(kappa_gt, filename = KAPPA_TBL_PNG, vwidth = TBL_W, vheight = TBL_H_KAPPA)
  message("Saved Kappa table PNG: ", KAPPA_TBL_PNG)

  readr::write_csv(kappa_tbl %>% dplyr::mutate(Dataset = as.character(Dataset)), KAPPA_CSV)
  message("Saved Kappa CSV: ", KAPPA_CSV)
} else {
  message("[KAPPA] No rows to render; skipping gt table.")
}