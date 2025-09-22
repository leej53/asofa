# =====================================================================
# File: 2b.Calibration_summary_statistics.R
#
# What this script does (for the README header)
# ---------------------------------------------
# This script computes and renders calibration summary statistics for
# 30-day in-hospital mortality comparing aSOFA vs SOFA across four cohorts.
# Specifically, it:
#
#   • Fits two logistic regression models per dataset (aSOFA total vs SOFA total).
#   • Computes Brier scores with 95% bootstrap confidence intervals (B = 1,000).
#   • Estimates calibration-in-the-large (CITL) and calibration slope (Wald CIs).
#   • Summarizes Δ Brier (aSOFA − SOFA).
#   • Renders a main gt-formatted table (PNG) and a supplementary table of
#     Brier skill scores (CSV + PNG).
#
# Four datasets/panels:
#   (1) MIMIC-IV Total cohort
#   (2) MIMIC-IV Sepsis (within 24h) cohort
#   (3) eICU-CRD Total cohort
#   (4) eICU-CRD Sepsis (within 24h) cohort
#
# How to use
# ----------
# 1. Edit the placeholders below:
#      USER_EMAIL   <- "YOUR_EMAIL_HERE"
#      PROJECT      <- "YOUR_PROJECT_ID"
#      MIMIC_DATASET<- "YOUR_MIMIC_DATASET"   # e.g. mimic_output
#      EICU_DATASET <- "YOUR_EICU_DATASET"    # e.g. eicu_output
#
# 2. Ensure the following tables are already built from the SQL scripts:
#      • <PROJECT>.<MIMIC_DATASET>.mimic_asofa_total
#      • <PROJECT>.<MIMIC_DATASET>.mimic_asofa_total_sepsis
#      • <PROJECT>.<EICU_DATASET>.eicu_asofa_total
#      • <PROJECT>.<EICU_DATASET>.eicu_asofa_total_sepsis
#
# 3. Required columns (queried below) must include:
#      mortality_30day, asofa_total_score, sofa_total_score
#    (These names match the SQL outputs.)
#
# Notes
# -----
# • Only the email/project/dataset/table names are meant to be edited by users.
# • All statistical routines and table-rendering code remain unchanged.
# =====================================================================

suppressPackageStartupMessages({
  req <- c("DBI","bigrquery","dplyr","tidyr","gt","stringr","glue","readr","purrr","webshot2")
  to_install <- req[!sapply(req, require, character.only = TRUE)]
  if (length(to_install)) install.packages(to_install, Ncpus = 2)
  invisible(lapply(req, library, character.only = TRUE))
})

set.seed(123)

# ==== knobs (portrait letter tuned) ====
B <- 1000
BASE_FAMILY <- "Helvetica"

OUT_DIR <- path.expand("~/Documents")
PNG_FILE <- file.path(OUT_DIR, "calibration_metrics_table.png")

PNG_W <- 2100
PNG_H <- 1100
# =======================================

# ---- User-configurable defaults ----
USER_EMAIL   <- "YOUR_EMAIL_HERE"
PROJECT      <- "YOUR_PROJECT_ID"
MIMIC_DATASET<- "YOUR_MIMIC_DATASET"
EICU_DATASET <- "YOUR_EICU_DATASET"
# ------------------------------------

# ---- BigQuery pull (4 tables, fixed order) ----
options(gargle_oauth_email = USER_EMAIL, gargle_oauth_cache = TRUE)
con <- DBI::dbConnect(
  bigrquery::bigquery(),
  project = PROJECT,
  billing = PROJECT,
  use_legacy_sql = FALSE
)

build_sql <- function(tbl){
  glue::glue("
    SELECT
      CAST(mortality_30day AS INT64)      AS y,
      CAST(asofa_total_score AS FLOAT64)  AS asofa_total,
      CAST(sofa_total_score  AS FLOAT64)  AS sofa_total
    FROM `{tbl}`
  ")
}
pull_df <- function(sql) DBI::dbGetQuery(con, sql)

q1 <- build_sql(glue::glue("{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total"))
q2 <- build_sql(glue::glue("{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total_sepsis"))
q3 <- build_sql(glue::glue("{PROJECT}.{EICU_DATASET}.eicu_asofa_total"))
q4 <- build_sql(glue::glue("{PROJECT}.{EICU_DATASET}.eicu_asofa_total_sepsis"))

dfs <- list(
  "MIMIC-IV"           = pull_df(q1),
  "MIMIC-IV (Sepsis)"  = pull_df(q2),
  "eICU-CRD"           = pull_df(q3),
  "eICU-CRD (Sepsis)"  = pull_df(q4)
)
DBI::dbDisconnect(con)

# ---- helpers ----

fmt <- function(x, k = 3) formatC(x, digits = k, format = "f", drop0trailing = TRUE)

glm_fit_pred <- function(df, score_col){
  dd <- df %>% dplyr::select(y, score = dplyr::all_of(score_col)) %>% tidyr::drop_na()
  fit <- glm(y ~ score, data = dd, family = binomial())
  p   <- as.numeric(predict(fit, newdata = dd, type = "response"))
  eps <- 1e-12
  p_c <- pmin(pmax(p, eps), 1 - eps)
  lp  <- qlogis(p_c)
  list(y = dd$y, p = p, lp = lp, se = (dd$y - p)^2, n = nrow(dd))
}

wald_citl_slope_safe <- function(y, lp){
  z <- qnorm(0.975)
  fit_c <- suppressWarnings(try(glm(y ~ 1 + offset(lp), family = binomial()), silent = TRUE))
  if (inherits(fit_c, "try-error")) { lp2 <- lp + rnorm(length(lp), sd = 1e-8); fit_c <- glm(y ~ 1 + offset(lp2), family = binomial()) }
  bc <- coef(fit_c)[["(Intercept)"]]
  vc <- try(vcov(fit_c)["(Intercept)","(Intercept)"], silent = TRUE)
  se_c <- if (inherits(vc, "try-error") || is.na(vc)) NA_real_ else sqrt(vc)

  fit_s <- suppressWarnings(try(glm(y ~ lp, family = binomial()), silent = TRUE))
  if (inherits(fit_s, "try-error")) { lp2 <- lp + rnorm(length(lp), sd = 1e-8); fit_s <- glm(y ~ lp2, family = binomial()); bs <- coef(fit_s)[["lp2"]]; vs <- try(vcov(fit_s)["lp2","lp2"], silent = TRUE) }
  else { bs <- coef(fit_s)[["lp"]]; vs <- try(vcov(fit_s)["lp","lp"], silent = TRUE) }
  se_s <- if (inherits(vs, "try-error") || is.na(vs)) NA_real_ else sqrt(vs)

  c(citl = unname(bc), citl_lo = unname(bc - z*se_c), citl_hi = unname(bc + z*se_c),
    slope = unname(bs), slope_lo = unname(bs - z*se_s), slope_hi = unname(bs + z*se_s))
}

boot_ci_mean_seq <- function(x, B, seed=2025){
  set.seed(seed); n <- length(x)
  bs <- replicate(B, mean(x[sample.int(n, n, replace=TRUE)], na.rm = TRUE))
  c(pt = mean(x), lo = unname(quantile(bs, 0.025, type=7)), hi = unname(quantile(bs, 0.975, type=7)))
}
boot_ci_diff_seq <- function(se_a, se_b, B, seed=2026){
  stopifnot(length(se_a) == length(se_b)); set.seed(seed); n <- length(se_a)
  bs <- replicate(B, { idx <- sample.int(n, n, replace=TRUE); mean(se_a[idx]) - mean(se_b[idx]) })
  c(pt = mean(se_a) - mean(se_b), lo = unname(quantile(bs, 0.025, type=7)), hi = unname(quantile(bs, 0.975, type=7)))
}

summarize_ds <- function(df, ds_label){
  base <- df %>% dplyr::select(y, asofa_total, sofa_total) %>% tidyr::drop_na()
  a <- glm_fit_pred(base %>% dplyr::select(y, asofa_total), "asofa_total")
  s <- glm_fit_pred(base %>% dplyr::select(y, sofa_total),  "sofa_total")
  n <- length(a$y); deaths <- sum(base$y)

  b_a <- boot_ci_mean_seq(a$se, B); b_s <- boot_ci_mean_seq(s$se, B)
  d_b <- boot_ci_diff_seq(a$se, s$se, B)
  cs_a <- wald_citl_slope_safe(a$y, a$lp)
  cs_s <- wald_citl_slope_safe(s$y, s$lp)

  tibble::tibble(
    Dataset = ds_label,
    Model   = c("aSOFA (total)", "SOFA (total)"),
    N       = n,
    Deaths  = deaths,
    Brier   = c(sprintf("%s [%s, %s]", fmt(b_a["pt"],4), fmt(b_a["lo"],4), fmt(b_a["hi"],4)),
                sprintf("%s [%s, %s]", fmt(b_s["pt"],4), fmt(b_s["lo"],4), fmt(b_s["hi"],4))),
    CITL    = c(sprintf("%s [%s, %s]", fmt(cs_a["citl"],3), fmt(cs_a["citl_lo"],3), fmt(cs_a["citl_hi"],3)),
                sprintf("%s [%s, %s]", fmt(cs_s["citl"],3), fmt(cs_s["citl_lo"],3), fmt(cs_s["citl_hi"],3))),
    Slope   = c(sprintf("%s [%s, %s]", fmt(cs_a["slope"],3), fmt(cs_a["slope_lo"],3), fmt(cs_a["slope_hi"],3)),
                sprintf("%s [%s, %s]", fmt(cs_s["slope"],3), fmt(cs_s["slope_lo"],3), fmt(cs_s["slope_hi"],3))),
    `Δ Brier (aSOFA−SOFA)` = c(sprintf("%s [%s, %s]", fmt(d_b["pt"],4), fmt(d_b["lo"],4), fmt(d_b["hi"],4)), "")
  )
}

# ---- compute & order ----
out_tbl <-
  purrr::imap_dfr(dfs, ~ summarize_ds(.x, .y)) %>%
  dplyr::arrange(factor(Dataset, levels = names(dfs)),
                 match(Model, c("aSOFA (total)", "SOFA (total)"))) %>%
  dplyr::select(Dataset, Model, N, Deaths, Brier, CITL, Slope, `Δ Brier (aSOFA−SOFA)`)

# ---- render with gt (portrait-letter tuned) ----
tbl_gt <-
  out_tbl |>
  gt::gt(groupname_col = "Dataset") |>
  gt::tab_header(
    title = gt::md("**Calibration Metrics for 30-day In-hospital Mortality (aSOFA vs SOFA)**"),
    subtitle = gt::md("One logistic fit per score; B = 1,000 bootstrap of squared errors; Wald CIs for CITL/Slope")
  ) |>
  gt::cols_label(
    Model = "Model",
    N = "N",
    Deaths = "Deaths",
    Brier = gt::html("Brier<br>(95% CI)"),
    CITL  = gt::html("CITL<br>(95% CI)"),
    Slope = gt::html("Slope<br>(95% CI)"),
    `Δ Brier (aSOFA−SOFA)` = gt::html("Δ Brier<br>(95% CI)")
  ) |>
  gt::cols_align(align = "center", columns = gt::everything()) |>
  # 폭 제어: 좁은 정보열은 좁게, CI 열은 중간 폭
  gt::cols_width(
    Model ~ gt::px(180),
    N ~ gt::px(90),
    Deaths ~ gt::px(90),
    Brier ~ gt::px(260),
    CITL  ~ gt::px(200),
    Slope ~ gt::px(200),
    `Δ Brier (aSOFA−SOFA)` ~ gt::px(220)
  ) |>
  gt::opt_row_striping() |>
  gt::tab_options(
    table.font.names = BASE_FAMILY,
    table.font.size  = gt::px(13),                # ≈ Word 10pt
    heading.align = "center",
    heading.title.font.size    = gt::px(16),
    heading.subtitle.font.size = gt::px(12),
    column_labels.font.weight  = "bold",
    column_labels.font.size    = gt::px(12),
    data_row.padding = gt::px(3),                 # 여백 축소로 세로 공간 절약
    table.width = gt::px(1200)                    # 표 자체 목표 폭 (~1200px)
  )

# ---- save (PNG) ----
gt::gtsave(tbl_gt, filename = PNG_FILE, vwidth = PNG_W, vheight = PNG_H)

# ==== Supplementary Brier skill score table (같은 폰트/폭 컨벤션) ====
make_supp_tbl <- function(df, ds_label){
  base <- df %>% dplyr::select(y, asofa_total, sofa_total) %>% tidyr::drop_na()
  n <- nrow(base); deaths <- sum(base$y); p <- deaths/n
  no_skill <- p * (1 - p)

  a <- glm_fit_pred(base %>% dplyr::select(y, asofa_total), "asofa_total")
  s <- glm_fit_pred(base %>% dplyr::select(y, sofa_total), "sofa_total")
  brier_a <- mean(a$se); brier_s <- mean(s$se)

  tibble::tibble(
    Dataset = ds_label,
    N = n,
    Deaths = deaths,
    `Event rate (%)` = round(100*p, 1),
    `No-skill Brier` = round(no_skill, 4),
    `Observed Brier (aSOFA)` = round(brier_a, 4),
    `Brier Skill Score (aSOFA)` = sprintf("%0.3f (%0.1f%%)", 1 - brier_a/no_skill, 100*(1 - brier_a/no_skill)),
    `Observed Brier (SOFA)` = round(brier_s, 4),
    `Brier Skill Score (SOFA)` = sprintf("%0.3f (%0.1f%%)", 1 - brier_s/no_skill, 100*(1 - brier_s/no_skill))
  )
}
supp_tbl <- purrr::imap_dfr(dfs, ~ make_supp_tbl(.x, .y))
readr::write_csv(supp_tbl, file.path(OUT_DIR, "supplement_brier_skill_table.csv"))

supp_gt <-
  supp_tbl |>
  gt::gt() |>
  gt::tab_header(
    title = gt::md("**Supplementary Table X. Event rates, no-skill Brier, and Brier skill scores**")
  ) |>
  gt::cols_align("center") |>
  gt::cols_width(
    Dataset ~ gt::px(220),
    N ~ gt::px(90),
    Deaths ~ gt::px(90),
    `Event rate (%)` ~ gt::px(140),
    `No-skill Brier` ~ gt::px(150),
    `Observed Brier (aSOFA)` ~ gt::px(180),
    `Brier Skill Score (aSOFA)` ~ gt::px(200),
    `Observed Brier (SOFA)` ~ gt::px(180),
    `Brier Skill Score (SOFA)` ~ gt::px(200)
  ) |>
  gt::opt_row_striping() |>
  gt::tab_options(
    table.font.names = BASE_FAMILY,
    table.font.size  = gt::px(13),
    column_labels.font.size = gt::px(12),
    data_row.padding = gt::px(3),
    table.width = gt::px(1200)
  )

gt::gtsave(supp_gt, file.path(OUT_DIR, "supplement_brier_skill_table.png"),
           vwidth = 2100, vheight = 700)
