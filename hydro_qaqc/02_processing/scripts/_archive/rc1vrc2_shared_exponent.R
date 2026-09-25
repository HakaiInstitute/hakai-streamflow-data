# =============================================================================
# SSN703 - RC1 vs RC2 Shared-Exponent Test (transition window: 2017-2020)
# =============================================================================
# Purpose:
#   Same test as 08_rc1_rc2_exponent_test.R, but restricted to gaugings from
#   2017-01-01 to 2020-01-01 -- a tight window straddling the actual RC1/RC2
#   transition (2018-09-14), rather than pooling RC1's full 2014-2018 history
#   (spanning both ssn703_a and ssn703_b) against RC2's full 2018-2022 span.
#
#   Motivation: the full-history fit (08) found a statistically indistinguishable
#   exponent (p = 0.729) but the fitted curves visibly diverged above ~150cm --
#   an area with very little data on either side, meaning h0 was poorly
#   constrained by extrapolation rather than by real high-stage measurements.
#   The implied offset (4.55cm) also did not match the direct paired-event
#   comparison from the historical stage recovery work (~14-24cm, growing with
#   stage). Restricting to a tighter, more hydrologically comparable window
#   removes the multi-sensor/multi-year pooling as a possible confound and
#   gives a more apples-to-apples test of the actual transition itself.
#
# Inputs:
#   03_docs/metadata/ssn703_gaugings_prepped.csv
#
# Outputs:
#   02_processing/plots/ssn703_rc1_rc2_exponent_test_2017_2020.pdf
#   Console: anova() result, fitted h0 offset, bootstrap CI on offset
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(minpack.lm)
library(boot)

meta_dir <- "03_docs/metadata"
plot_dir <- "02_processing/plots"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)

WINDOW_START <- as.POSIXct("2017-01-01 00:00:00", tz = "Etc/GMT+8")
WINDOW_END   <- as.POSIXct("2020-01-01 00:00:00", tz = "Etc/GMT+8")


# -----------------------------------------------------------------------------
# 1. Load and filter gaugings
# -----------------------------------------------------------------------------
# Restrict to RC1/RC2, stage_status == "ok", and the 2017-2020 transition
# window. Final_rating_curve is NOT used as a filter -- see 08's notes on why
# (RC2 gaugings are essentially never flagged "Y"; that column reflects the
# older manually-curated RC1 workflow, not RC2's LOESS-based fitting process).
#
# NOTE: gaugings$datetime is UTC -- window bounds above are defined in PST
# for readability, so convert before filtering to avoid an 8-hour boundary
# mismatch (small at this scale, but worth doing correctly given how many
# timezone mistakes this dataset has already produced).

gaugings <- read_csv(file.path(meta_dir, "ssn703_gaugings_prepped.csv"),
                      show_col_types = FALSE)

window_start_utc <- with_tz(WINDOW_START, tzone = "UTC")
window_end_utc   <- with_tz(WINDOW_END,   tzone = "UTC")

fit_data <- gaugings |>
  filter(
    rating_curve_period %in% c("RC1", "RC2"),
    stage_status == "ok",
    datetime >= window_start_utc,
    datetime <  window_end_utc,
    !is.na(Stage_avg_corrected), !is.na(Q_meas)
  ) |>
  mutate(rating_curve_period = factor(rating_curve_period, levels = c("RC1", "RC2")))

message("Gaugings used for fitting (2017-01-01 to 2020-01-01 window): ")
fit_data |> count(rating_curve_period) |> print()

n_by_period <- fit_data |> count(rating_curve_period) |> pull(n)
if (any(n_by_period < 8)) {
  warning("One or both periods have very few gaugings (<8) in this window -- ",
          "exponent fit may be unstable. Consider widening the window if this fails to converge.")
}
if (any(n_by_period == 0)) {
  stop("One period has zero gaugings in this window -- nlsLM will fail on a ",
       "non-finite starting value. Check the count above and widen the window if needed.")
}


# -----------------------------------------------------------------------------
# 2. Fit models
# -----------------------------------------------------------------------------
# Explicit ifelse()-based scalar parameters per group -- indexed vector
# parameters (a[group]) are not reliably supported by nlsLM's LM backend
# (see 08's notes -- this caused a hessian dimension-mismatch error there).

h0_start <- fit_data |>
  group_by(rating_curve_period) |>
  summarise(h0_guess = min(Stage_avg_corrected) - 5, .groups = "drop") |>
  pull(h0_guess, name = rating_curve_period)

message("\nStarting h0 guesses (5cm below min observed stage per period): ")
print(h0_start)

m_shared_b <- nlsLM(
  Q_meas ~ ifelse(rating_curve_period == "RC1", a1, a2) *
           pmax(Stage_avg_corrected - ifelse(rating_curve_period == "RC1", h01, h02), 1e-6)^b,
  data = fit_data,
  start = list(a1 = 1, a2 = 1,
               h01 = unname(h0_start["RC1"]), h02 = unname(h0_start["RC2"]),
               b = 1.5),
  control = nls.lm.control(maxiter = 500)
)

m_separate <- nlsLM(
  Q_meas ~ ifelse(rating_curve_period == "RC1", a1, a2) *
           pmax(Stage_avg_corrected - ifelse(rating_curve_period == "RC1", h01, h02), 1e-6)^
           ifelse(rating_curve_period == "RC1", b1, b2),
  data = fit_data,
  start = list(a1 = 1, a2 = 1,
               h01 = unname(h0_start["RC1"]), h02 = unname(h0_start["RC2"]),
               b1 = 1.5, b2 = 1.5),
  control = nls.lm.control(maxiter = 500)
)

message("\n--- m_shared_b summary ---")
print(summary(m_shared_b))

message("\n--- m_separate summary ---")
print(summary(m_separate))


# -----------------------------------------------------------------------------
# 3. Nested model comparison
# -----------------------------------------------------------------------------

message("\n--- ANOVA: shared b vs separate b (2017-2020 window) ---")
anova_result <- anova(m_shared_b, m_separate)
print(anova_result)

p_value <- anova_result[["Pr(>F)"]][2]
message("\np-value: ", round(p_value, 4))
if (p_value < 0.05) {
  message("SIGNIFICANT: exponents differ -- evidence AGAINST shared hydraulic control")
} else {
  message("NOT significant: exponents statistically indistinguishable -- ",
          "consistent with (does not prove) shared hydraulic control, ",
          "different datum")
}


# -----------------------------------------------------------------------------
# 4. Implied datum offset from the shared-b model
# -----------------------------------------------------------------------------

h0_coefs <- coef(m_shared_b)[c("h01", "h02")]
offset_estimate <- h0_coefs["h02"] - h0_coefs["h01"]

message("\nFitted h0 (effective stage of zero flow) per period:")
message("  RC1 (h01): ", round(h0_coefs["h01"], 2))
message("  RC2 (h02): ", round(h0_coefs["h02"], 2))
message("\nImplied datum offset (h0_RC2 - h0_RC1): ", round(offset_estimate, 2), " cm")
message("Compare against the direct paired-event comparison from the historical",
        "\nstage recovery work (~14-24cm, growing with stage) -- large disagreement",
        "\nbetween the two would suggest h0 is still poorly constrained even in",
        "\nthis narrower window.")


# -----------------------------------------------------------------------------
# 5. Bootstrap CI on the offset
# -----------------------------------------------------------------------------

boot_offset <- function(data, indices) {
  d <- data[indices, ]
  fit <- tryCatch(
    nlsLM(
      Q_meas ~ ifelse(rating_curve_period == "RC1", a1, a2) *
               pmax(Stage_avg_corrected - ifelse(rating_curve_period == "RC1", h01, h02), 1e-6)^b,
      data = d,
      start = list(a1 = 1, a2 = 1,
                   h01 = unname(h0_start["RC1"]), h02 = unname(h0_start["RC2"]),
                   b = 1.5),
      control = nls.lm.control(maxiter = 500)
    ),
    error = function(e) NULL
  )
  if (is.null(fit)) return(NA_real_)
  ho <- coef(fit)[c("h01", "h02")]
  unname(ho["h02"] - ho["h01"])
}

message("\nRunning bootstrap (R = 1000, may take a moment)...")
set.seed(703)
boot_result <- boot(fit_data, boot_offset, R = 1000)

n_failed <- sum(is.na(boot_result$t))
message("Bootstrap replicates failed to converge: ", n_failed, " of 1000")
if (n_failed > 200) {
  message("NOTE: high failure rate suggests the model is poorly constrained in this ",
          "window too -- treat the point estimate and CI with caution.")
}

boot_ci <- tryCatch(
  boot.ci(boot_result, type = "perc"),
  error = function(e) {
    message("boot.ci() failed -- likely too many NA replicates; check n_failed above")
    NULL
  }
)

if (!is.null(boot_ci)) {
  message("\nBootstrap 95% CI on offset (h0_RC2 - h0_RC1):")
  print(boot_ci)
}


# -----------------------------------------------------------------------------
# 6. Diagnostic plot
# -----------------------------------------------------------------------------

stage_range <- seq(min(fit_data$Stage_avg_corrected), max(fit_data$Stage_avg_corrected), length.out = 200)

pred_shared <- bind_rows(
  tibble(rating_curve_period = "RC1",
         Stage_avg_corrected = stage_range,
         Q_pred = coef(m_shared_b)["a1"] * pmax(stage_range - coef(m_shared_b)["h01"], 0)^coef(m_shared_b)["b"]),
  tibble(rating_curve_period = "RC2",
         Stage_avg_corrected = stage_range,
         Q_pred = coef(m_shared_b)["a2"] * pmax(stage_range - coef(m_shared_b)["h02"], 0)^coef(m_shared_b)["b"])
)

p <- ggplot() +
  geom_point(data = fit_data, aes(x = Stage_avg_corrected, y = Q_meas, colour = rating_curve_period),
             alpha = 0.6, size = 2) +
  geom_line(data = pred_shared, aes(x = Stage_avg_corrected, y = Q_pred, colour = rating_curve_period),
            linewidth = 0.8) +
  scale_colour_manual(values = c("RC1" = "#E41A1C", "RC2" = "#4DAF4A")) +
  labs(
    title    = "SSN703 -- RC1 vs RC2, shared-exponent fit (2017-2020 transition window)",
    subtitle = paste0("Shared b = ", round(coef(m_shared_b)["b"], 3),
                      " | Implied offset (h0_RC2 - h0_RC1) = ", round(offset_estimate, 2), " cm",
                      " | ANOVA p = ", round(p_value, 4),
                      " | n = ", nrow(fit_data)),
    x        = "Stage corrected (cm)",
    y        = "Discharge (m³/s)",
    colour   = "RC period",
    caption  = paste0(
      "Points = observed gaugings, 2017-01-01 to 2020-01-01 only (post RC1-tail correction)\n",
      "Lines = shared-b model fit | Generated by 08b_rc1_rc2_exponent_test_2017_2020.R"
    )
  ) +
  theme_bw() +
  theme(legend.position = "bottom", panel.grid.minor = element_blank())

out_path <- file.path(plot_dir, "ssn703_rc1_rc2_exponent_test_2017_2020.pdf")
pdf(out_path, width = 10, height = 7)
print(p)
dev.off()

message("\nSaved: ", out_path)
message("\nDone. Compare this result against the full-history version (08) --")
message("if the offset estimate and curve divergence both settle down in this")
message("narrower window, that supports the earlier result being distorted by")
message("pooling across sensor generations / years rather than reflecting the")
message("transition itself.")