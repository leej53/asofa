# =====================================================================
# File: 2a.Calibration.R
#
# What this script does (for the README header)
# ---------------------------------------------
# This script produces a 4-in-1 calibration figure comparing aSOFA vs SOFA
# for 30-day in-hospital mortality. For each dataset/panel, it:
#
#   • Fits two logistic models (outcome = 30-day mortality; predictors =
#     total aSOFA and total SOFA, separately) to obtain predicted probabilities.
#   • Bins predictions into quantiles, plots bin means (observed vs predicted),
#     and overlays a LOESS calibration curve; the 45° dashed line indicates
#     perfect calibration.
#   • Renders four panels: (1) MIMIC-IV Total cohort,
#                          (2) MIMIC-IV Sepsis (within first 24h) cohort,
#                          (3) eICU-CRD Total cohort,
#                          (4) eICU-CRD Sepsis (within first 24h) cohort.
#
# How to use
# ----------
# 1. Edit the three placeholders at the top:
#      USER_EMAIL   <- "YOUR_EMAIL_HERE"
#      PROJECT      <- "YOUR_PROJECT_ID"
#      MIMIC_DATASET<- "YOUR_MIMIC_DATASET"   # e.g. mimic_output
#      EICU_DATASET <- "YOUR_EICU_DATASET"    # e.g. eicu_output
#
# 2. Make sure you have already built the following tables via the provided
#    SQL scripts (schemas/column names come directly from those scripts):
#      • MIMIC total:  <PROJECT>.<MIMIC_DATASET>.mimic_asofa_total
#      • MIMIC sepsis: <PROJECT>.<MIMIC_DATASET>.mimic_asofa_total_sepsis
#      • eICU total:   <PROJECT>.<EICU_DATASET>.eicu_asofa_total
#      • eICU sepsis:  <PROJECT>.<EICU_DATASET>.eicu_asofa_total_sepsis
#
# 3. Required columns (queried below) must include:
#      mortality_30day, asofa_total_score, sofa_total_score
#    (These names match the tables built by the SQL above.)
#
# Notes
# -----
# • Only the email/project/dataset/table names are meant to be edited by users.
# • All statistical logic and plotting routines remain unchanged from the
#   validated source script.
# =========================
# 4-in-1 Calibration Figure
# =========================
suppressPackageStartupMessages({
  library(DBI); library(bigrquery)
  library(dplyr); library(tidyr)
  library(ggplot2); library(scales)
  library(glue); library(tibble)
  library(patchwork)
})

set.seed(123)

# ==== knobs ====
BINS <- 20
LOESS_SPAN <- 0.75
BASE_FAMILY <- "Helvetica"
BASE_SIZE <- 11              # 최소 Word 10pt 이상 가독성 확보
FIG_WIDTH <- 7.5             # in (Letter 본문 폭에 맞춤)
FIG_HEIGHT <- 7.5            # in
DPI <- 300
CAPTION_TXT <- "Points: bin means (size ∝ bin n). Solid line: LOESS. Dashed: ideal."
OUT_PNG <- "~/Documents/calibration_4in1_asofa_vs_sofa.png"
# ==============

# ---- User-configurable defaults (edit here) ----
# Replace with your email/project/datasets. Everything else stays the same.
USER_EMAIL   <- "YOUR_EMAIL_HERE"
PROJECT      <- "YOUR_PROJECT_ID"
MIMIC_DATASET<- "YOUR_MIMIC_DATASET"  # e.g., "mimic_output"
EICU_DATASET <- "YOUR_EICU_DATASET"   # e.g., "eicu_output"
# ------------------------------------------------

# ---- BigQuery ----
options(gargle_oauth_email = USER_EMAIL, gargle_oauth_cache = TRUE)

con <- dbConnect(
  bigrquery::bigquery(),
  project = PROJECT,
  billing = PROJECT,
  use_legacy_sql = FALSE
)

# Table names and headers aligned to the released SQL scripts:
# - mimic_asofa_total / mimic_asofa_total_sepsis
# - eicu_asofa_total  / eicu_asofa_total_sepsis
# - Columns: mortality_30day, asofa_total_score, sofa_total_score

sql_mimic_total <- glue::glue("
SELECT
  CAST(mortality_30day   AS INT64)   AS y,
  CAST(asofa_total_score AS FLOAT64) AS asofa_total,
  CAST(sofa_total_score  AS FLOAT64) AS sofa_total
FROM `{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total`
")

sql_mimic_sepsis <- glue::glue("
SELECT
  CAST(mortality_30day   AS INT64)   AS y,
  CAST(asofa_total_score AS FLOAT64) AS asofa_total,
  CAST(sofa_total_score  AS FLOAT64) AS sofa_total
FROM `{PROJECT}.{MIMIC_DATASET}.mimic_asofa_total_sepsis`
")

sql_eicu_total <- glue::glue("
SELECT
  CAST(mortality_30day   AS INT64)   AS y,
  CAST(asofa_total_score AS FLOAT64) AS asofa_total,
  CAST(sofa_total_score  AS FLOAT64) AS sofa_total
FROM `{PROJECT}.{EICU_DATASET}.eicu_asofa_total`
")

sql_eicu_sepsis <- glue::glue("
SELECT
  CAST(mortality_30day   AS INT64)   AS y,
  CAST(asofa_total_score AS FLOAT64) AS asofa_total,
  CAST(sofa_total_score  AS FLOAT64) AS sofa_total
FROM `{PROJECT}.{EICU_DATASET}.eicu_asofa_total_sepsis`
")

mimic_total  <- DBI::dbGetQuery(con, sql_mimic_total)
mimic_sepsis <- DBI::dbGetQuery(con, sql_mimic_sepsis)
eicu_total   <- DBI::dbGetQuery(con, sql_eicu_total)
eicu_sepsis  <- DBI::dbGetQuery(con, sql_eicu_sepsis)

DBI::dbDisconnect(con)

# ---- helpers ----
glm_fit_pred <- function(df, score_col){
  dd <- df %>% select(y, score = all_of(score_col)) %>% tidyr::drop_na()
  fit <- glm(y ~ score, data = dd, family = binomial())
  p <- as.numeric(predict(fit, type = "response"))
  list(y = dd$y, p = p)
}

make_bins <- function(p, bins) {
  pj <- jitter(p, factor = 1e-12)
  brks <- as.numeric(quantile(pj, probs = seq(0, 1, length.out = bins + 1),
                              type = 8, na.rm = TRUE))
  brks[1] <- min(0, brks[1], na.rm = TRUE)
  brks[length(brks)] <- max(1, brks[length(brks)], na.rm = TRUE)
  cut(pj, breaks = brks, include.lowest = TRUE, labels = FALSE)
}

calib_bins_curve <- function(p, y, bins = BINS, span = LOESS_SPAN){
  bin_id <- make_bins(p, bins)
  d <- tibble(pred = p, y = y, bin = bin_id)
  bins_tbl <- d %>%
    group_by(bin) %>%
    summarise(n = n(),
              pred_mean = mean(pred),
              obs_rate  = mean(y),
              .groups = "drop") %>%
    arrange(pred_mean)
  grid <- data.frame(pred = seq(min(bins_tbl$pred_mean), max(bins_tbl$pred_mean), length.out = 201))
  lo <- try(loess(obs_rate ~ pred_mean, data = bins_tbl,
                  span = span, control = loess.control(surface = "direct")), silent = TRUE)
  curve <- if (inherits(lo, "try-error")) {
    bins_tbl %>% transmute(x = pred_mean, y = obs_rate)
  } else {
    data.frame(x = grid$pred,
               y = predict(lo, newdata = data.frame(pred_mean = grid$pred)))
  }
  list(points = bins_tbl, curve = curve)
}

prep_two_models <- function(df) {
  base <- df %>% select(y, asofa_total, sofa_total) %>% tidyr::drop_na()
  asofa <- glm_fit_pred(base %>% select(y, asofa_total) %>% rename(score = asofa_total), "score")
  sofa  <- glm_fit_pred(base %>% select(y, sofa_total)  %>% rename(score = sofa_total),  "score")
  ca <- calib_bins_curve(asofa$p, asofa$y)
  cs <- calib_bins_curve(sofa$p,  sofa$y)
  bins_tbl <- bind_rows(
    ca$points %>% mutate(Model = "aSOFA"),
    cs$points %>% mutate(Model = "SOFA")
  )
  curve_tbl <- bind_rows(
    ca$curve %>% mutate(Model = "aSOFA"),
    cs$curve %>% mutate(Model = "SOFA")
  )
  list(bins = bins_tbl, curve = curve_tbl)
}

calib_panel <- function(df, subtitle_txt){
  dd <- prep_two_models(df)
  ggplot() +
    geom_abline(slope = 1, intercept = 0, linetype = 2, linewidth = 0.5) +
    geom_point(data = dd$bins,
               aes(x = pred_mean, y = obs_rate, size = n, shape = Model, color = Model),
               alpha = 0.75) +
    geom_line(data = dd$curve, aes(x = x, y = y, color = Model, linetype = Model), linewidth = 1.1) +
    scale_color_manual(values = c("aSOFA" = "#0072B2", "SOFA" = "#E69F00")) +
    scale_shape_manual(values = c("aSOFA" = 16, "SOFA" = 17)) +
    scale_linetype_manual(values = c("aSOFA" = "solid", "SOFA" = "dashed")) +
    scale_x_continuous(limits = c(0,1), labels = percent_format(accuracy = 1)) +
    scale_y_continuous(limits = c(0,1), labels = percent_format(accuracy = 1)) +
    scale_size(range = c(1.8, 4.5), guide = "none") +
    labs(x = "Predicted 30-day mortality",
         y = "Observed 30-day mortality",
         subtitle = subtitle_txt) +
    coord_equal() +
    theme_minimal(base_size = BASE_SIZE, base_family = BASE_FAMILY) +
    theme(
      legend.position = "bottom",
      plot.subtitle = element_text(face = "bold"),
      panel.grid.minor = element_blank()
    )
}

# ---- Build 4 panels ----
p_mimic_total  <- calib_panel(mimic_total,  "MIMIC-IV • Total")
p_mimic_sepsis <- calib_panel(mimic_sepsis, "MIMIC-IV • Sepsis (first 24h)")
p_eicu_total   <- calib_panel(eicu_total,   "eICU-CRD • Total")
p_eicu_sepsis  <- calib_panel(eicu_sepsis,  "eICU-CRD • Sepsis (first 24h)")

# ---- Combine with patchwork ----
four <- (p_mimic_total | p_mimic_sepsis) /
        (p_eicu_total  | p_eicu_sepsis) +
        patchwork::plot_layout(guides = "collect") +
        patchwork::plot_annotation(
          title = "Calibration of aSOFA vs SOFA for 30-day In-hospital Mortality",
          caption = CAPTION_TXT,
          theme = theme(
            plot.title = element_text(size = BASE_SIZE + 1, face = "bold",
                                      family = BASE_FAMILY, hjust = 0.5),
            plot.caption = element_text(size = BASE_SIZE - 1, family = BASE_FAMILY)
          )
        )

four <- four & theme(legend.position = "bottom")

# ---- Save ----
ggsave(OUT_PNG, four, width = FIG_WIDTH, height = FIG_HEIGHT, dpi = DPI)
message("Saved: ", OUT_PNG)
