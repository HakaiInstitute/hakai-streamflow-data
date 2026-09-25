# =============================================================================
# SSN703 - RC1 vs RC2 Exponent Test: Robustness Checks
# =============================================================================
# Purpose:
#   Stress-test the 2017-2020 finding that RC1 and RC2 have significantly
#   different power-law exponents (b1 = 1.66, b2 = 2.26, ANOVA p = 0.0032)
#   before treating it as a settled conclusion. Three checks:
#
#   1. Leave-one-out influence: refit with each gauging removed in turn.
#      If the significance or exponent estimates depend heavily on one or
#      two points, that's a fragility worth knowing before reporting this
#      as a robust finding.
#   2. Residual diagnostics: look for systematic patterns (vs stage, vs
#      time, vs RC period) that would suggest the model form itself is
#      misspecified rather than the two curves genuinely differing.
#   3. Window sensitivity: refit under a few alternative window definitions
#      around the 2018-09-14 transition. If the exponent difference and its
#      significance hold up across reasonable window choices, that's much
#      stronger evidence than any single window result on its own.
#
# Inputs:
#   03_docs/metadata/ssn703_gaugings_prepped.csv
#
# Outputs:
#   02_processing/plots/ssn703_rc1_rc2_robustness_loo.pdf
#   02_processing/plots/ssn703_rc1_rc2_robustness_residuals.pdf
#   Console: window sensitivity table
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(minpack.lm)

meta_dir <- "03_docs/metadata"
plot_dir <- "02_processing/plots"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)

gaugings <- read_csv(file.path(meta_dir, "ssn703_gaugings_prepped.csv"),
                      show_col_types = FALSE)


# -----------------------------------------------------------------------------
# Helper: fit both models for a given data subset, return key results
# -----------------------------------------------------------------------------
# Returns NULL (rather than erroring) if fitting fails, so this can be used
# safely inside a loop without stopping the whole robustness check on one
# bad subset.

fit_exponent_test <- function(fit_data) {
  n_by_period <- fit_data |> count(rating_curve_period) |> pull(n)
  if (length(n_by_period) < 2 || any(n_by_period < 5)) return(NULL)

  h0_start <- fit_data |>
    group_by(rating_curve_period) |>
    summarise(h0_guess = min(Stage_avg_corrected) - 5, .groups = "drop") |>
    pull(h0_guess, name = rating_curve_period)

  result <- tryCatch({
    m_shared <- nlsLM(
      Q_meas ~ ifelse(rating_curve_period == "RC1", a1, a2) *
               pmax(Stage_avg_corrected - ifelse(rating_curve_period == "RC1", h01, h02), 1e-6)^b,
      data = fit_data,
      start = list(a1 = 1, a2 = 1,
                   h01 = unname(h0_start["RC1"]), h02 = unname(h0_start["RC2"]),
                   b = 1.5),
      control = nls.lm.control(maxiter = 500)
    )
    m_sep <- nlsLM(
      Q_meas ~ ifelse(rating_curve_period == "RC1", a1, a2) *
               pmax(Stage_avg_corrected - ifelse(rating_curve_period == "RC1", h01, h02), 1e-6)^
               ifelse(rating_curve_period == "RC1", b1, b2),
      data = fit_data,
      start = list(a1 = 1, a2 = 1,
                   h01 = unname(h0_start["RC1"]), h02 = unname(h0_start["RC2"]),
                   b1 = 1.5, b2 = 1.5),
      control = nls.lm.control(maxiter = 500)
    )
    av <- anova(m_shared, m_sep)
    list(
      p_value = av[["Pr(>F)"]][2],
      b1 = unname(coef(m_sep)["b1"]),
      b2 = unname(coef(m_sep)["b2"]),
      n_rc1 = n_by_period[1],
      n_rc2 = n_by_period[2],
      converged = TRUE
    )
  }, error = function(e) NULL)

  result
}


# -----------------------------------------------------------------------------
# 1. Leave-one-out influence check (2017-2020 window, matching 08b)
# -----------------------------------------------------------------------------

WINDOW_START <- as.POSIXct("2017-01-01 00:00:00", tz = "Etc/GMT+8")
WINDOW_END   <- as.POSIXct("2020-01-01 00:00:00", tz = "Etc/GMT+8")

base_data <- gaugings |>
  filter(
    rating_curve_period %in% c("RC1", "RC2"),
    stage_status == "ok",
    datetime >= with_tz(WINDOW_START, "UTC"),
    datetime <  with_tz(WINDOW_END, "UTC"),
    !is.na(Stage_avg_corrected), !is.na(Q_meas)
  ) |>
  mutate(rating_curve_period = factor(rating_curve_period, levels = c("RC1", "RC2")),
         row_id = row_number())

message("Base dataset for LOO check: ", nrow(base_data), " gaugings")
message("Running leave-one-out (this refits ", nrow(base_data), " times, may take a few minutes)...")

loo_results <- map_dfr(base_data$row_id, function(i) {
  subset_data <- base_data |> filter(row_id != i)
  fit <- fit_exponent_test(subset_data)
  if (is.null(fit)) {
    return(tibble(row_id = i, p_value = NA, b1 = NA, b2 = NA, converged = FALSE))
  }
  tibble(row_id = i, p_value = fit$p_value, b1 = fit$b1, b2 = fit$b2, converged = TRUE)
})

n_loo_failed <- sum(!loo_results$converged)
message("LOO refits that failed to converge: ", n_loo_failed, " of ", nrow(base_data))

loo_results <- loo_results |>
  left_join(base_data |> select(row_id, EventID, MID, datetime, rating_curve_period,
                                 Stage_avg_corrected, Q_meas),
            by = "row_id")

# How many LOO refits flip the significance conclusion (using p < 0.05 as the line)?
n_flip <- sum(loo_results$p_value >= 0.05, na.rm = TRUE)
message("\nLOO refits where removing ONE point flips the result to non-significant: ",
        n_flip, " of ", sum(loo_results$converged))

if (n_flip > 0) {
  message("These points are worth a closer look -- each one alone is enough to change the conclusion:")
  loo_results |> filter(p_value >= 0.05) |>
    select(EventID, MID, datetime, rating_curve_period, Stage_avg_corrected, Q_meas, p_value) |>
    print()
} else {
  message("No single point removal flips the significance conclusion -- result appears robust to individual points.")
}

message("\nRange of b1 (RC1 exponent) across all LOO refits: ",
        round(min(loo_results$b1, na.rm = TRUE), 3), " to ", round(max(loo_results$b1, na.rm = TRUE), 3),
        " (full-data estimate: 1.66)")
message("Range of b2 (RC2 exponent) across all LOO refits: ",
        round(min(loo_results$b2, na.rm = TRUE), 3), " to ", round(max(loo_results$b2, na.rm = TRUE), 3),
        " (full-data estimate: 2.26)")

# Plot: p-value across LOO refits, with the significance line marked
p_loo <- loo_results |>
  filter(converged) |>
  mutate(point_label = paste0(rating_curve_period, ": ", round(Stage_avg_corrected, 0), "cm")) |>
  ggplot(aes(x = reorder(point_label, p_value), y = p_value, colour = rating_curve_period)) +
  geom_point() +
  geom_hline(yintercept = 0.05, linetype = "dashed", colour = "red") +
  scale_colour_manual(values = c("RC1" = "#E41A1C", "RC2" = "#4DAF4A")) +
  labs(
    title = "Leave-one-out sensitivity: ANOVA p-value with each point removed",
    subtitle = "Points below the red line still support 'exponents differ' even with that gauging excluded",
    x = "Gauging removed (RC period: stage)", y = "p-value (shared vs separate exponent)"
  ) +
  theme_bw() +
  theme(axis.text.x = element_blank(), axis.ticks.x = element_blank(), legend.position = "bottom")

pdf(file.path(plot_dir, "ssn703_rc1_rc2_robustness_loo.pdf"), width = 10, height = 6)
print(p_loo)
dev.off()
message("\nSaved: ", file.path(plot_dir, "ssn703_rc1_rc2_robustness_loo.pdf"))


# -----------------------------------------------------------------------------
# 2. Residual diagnostics (full 2017-2020 model)
# -----------------------------------------------------------------------------

full_fit <- fit_exponent_test(base_data)

h0_start <- base_data |>
  group_by(rating_curve_period) |>
  summarise(h0_guess = min(Stage_avg_corrected) - 5, .groups = "drop") |>
  pull(h0_guess, name = rating_curve_period)

m_sep_full <- nlsLM(
  Q_meas ~ ifelse(rating_curve_period == "RC1", a1, a2) *
           pmax(Stage_avg_corrected - ifelse(rating_curve_period == "RC1", h01, h02), 1e-6)^
           ifelse(rating_curve_period == "RC1", b1, b2),
  data = base_data,
  start = list(a1 = 1, a2 = 1,
               h01 = unname(h0_start["RC1"]), h02 = unname(h0_start["RC2"]),
               b1 = 1.5, b2 = 1.5),
  control = nls.lm.control(maxiter = 500)
)

base_data_resid <- base_data |>
  mutate(
    fitted_q = fitted(m_sep_full),
    residual = residuals(m_sep_full),
    std_residual = residual / sd(residual)
  )

p_resid_stage <- ggplot(base_data_resid, aes(x = Stage_avg_corrected, y = std_residual, colour = rating_curve_period)) +
  geom_point(size = 2) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_hline(yintercept = c(-2, 2), linetype = "dotted", colour = "grey50") +
  scale_colour_manual(values = c("RC1" = "#E41A1C", "RC2" = "#4DAF4A")) +
  labs(title = "Standardized residuals vs stage (separate-exponent model)",
       subtitle = "Look for: any systematic curve/trend in residuals (model misspecification), or a cluster of large residuals (outlier events)",
       x = "Stage corrected (cm)", y = "Standardized residual") +
  theme_bw() + theme(legend.position = "bottom")

p_resid_time <- ggplot(base_data_resid, aes(x = datetime, y = std_residual, colour = rating_curve_period)) +
  geom_point(size = 2) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_hline(yintercept = c(-2, 2), linetype = "dotted", colour = "grey50") +
  scale_colour_manual(values = c("RC1" = "#E41A1C", "RC2" = "#4DAF4A")) +
  labs(title = "Standardized residuals vs time",
       subtitle = "Look for: drift or a step-change in residuals near the transition date itself",
       x = NULL, y = "Standardized residual") +
  theme_bw() + theme(legend.position = "bottom")

pdf(file.path(plot_dir, "ssn703_rc1_rc2_robustness_residuals.pdf"), width = 10, height = 10)
print(p_resid_stage)
print(p_resid_time)
dev.off()
message("Saved: ", file.path(plot_dir, "ssn703_rc1_rc2_robustness_residuals.pdf"))

n_large_resid <- sum(abs(base_data_resid$std_residual) > 2)
message("\nGaugings with |standardized residual| > 2: ", n_large_resid, " of ", nrow(base_data_resid))
if (n_large_resid > 0) {
  base_data_resid |> filter(abs(std_residual) > 2) |>
    select(EventID, MID, datetime, rating_curve_period, Stage_avg_corrected, Q_meas, std_residual) |>
    print()
}


# -----------------------------------------------------------------------------
# 3. Window sensitivity
# -----------------------------------------------------------------------------

windows <- tribble(
  ~label,               ~start,       ~end,
  "2016-2021 (wide)",   "2016-01-01", "2021-01-01",
  "2017-2020 (base)",   "2017-01-01", "2020-01-01",
  "2018-2021 (shift)",  "2018-01-01", "2021-01-01",
  "2017-2019 (tight)",  "2017-01-01", "2019-06-01"
)

message("\n--- Window sensitivity ---")

window_results <- windows |>
  mutate(start = as.POSIXct(start, tz = "Etc/GMT+8"),
         end   = as.POSIXct(end, tz = "Etc/GMT+8")) |>
  rowwise() |>
  mutate(
    fit_summary = list({
      window_data <- gaugings |>
        filter(
          rating_curve_period %in% c("RC1", "RC2"),
          stage_status == "ok",
          datetime >= with_tz(start, "UTC"),
          datetime <  with_tz(end, "UTC"),
          !is.na(Stage_avg_corrected), !is.na(Q_meas)
        ) |>
        mutate(rating_curve_period = factor(rating_curve_period, levels = c("RC1", "RC2")))
      fit_exponent_test(window_data)
    })
  ) |>
  ungroup()

window_summary <- window_results |>
  mutate(
    n_rc1 = map_dbl(fit_summary, ~ if (is.null(.x)) NA else .x$n_rc1),
    n_rc2 = map_dbl(fit_summary, ~ if (is.null(.x)) NA else .x$n_rc2),
    p_value = map_dbl(fit_summary, ~ if (is.null(.x)) NA else .x$p_value),
    b1 = map_dbl(fit_summary, ~ if (is.null(.x)) NA else .x$b1),
    b2 = map_dbl(fit_summary, ~ if (is.null(.x)) NA else .x$b2)
  ) |>
  select(label, n_rc1, n_rc2, b1, b2, p_value)

print(window_summary)

n_windows_significant <- sum(window_summary$p_value < 0.05, na.rm = TRUE)
n_windows_tested <- sum(!is.na(window_summary$p_value))
message("\nSignificant (p < 0.05) in ", n_windows_significant, " of ", n_windows_tested, " window variants tested")

message("\nDone. Review:")
message("  1. LOO plot -- are all points below the p=0.05 line? (robust to individual points)")
message("  2. Residual plots -- any systematic pattern vs stage or time?")
message("  3. Window table -- does significance and the b1/b2 gap hold up across window choices?")