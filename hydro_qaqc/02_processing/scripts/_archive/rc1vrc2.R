# =============================================================================
# SSN703 - RC1 vs RC2 Shared-Exponent Test
# =============================================================================
# Purpose:
#   Test whether the RC1 (loc_1, ssn703_a/b) and RC2 (loc_2, ssn703_c) rating
#   curves share the same power-law exponent (b) -- i.e. whether the apparent
#   difference between them is consistent with a pure datum/offset difference
#   (same hydraulic control, different sensor elevation) or reflects a
#   genuine difference in control geometry.
#
#   This directly tests the claim in the v5 methodology doc that the RC1/RC2
#   boundary step change "reflects the genuine hydraulic difference between
#   loc_1 and loc_2 and is not a processing artefact" -- which was asserted
#   without a shown statistical basis.
#
#   Now that ssn703_gaugings_prepped.csv has been corrected for the
#   2018-09-14 to 2019-02-09 provenance issue (07_recover_historical_stage_
#   rc1_tail.R), the RC1 gauging set is trustworthy and this test can be
#   run properly.
#
# Model:
#   Q = a * (h - h0)^b
#
#   m_shared_b:   b shared across RC1/RC2, a and h0 free per period
#                 -- null hypothesis: same control, different datum
#   m_separate:   a, h0, AND b all free per period
#                 -- alternative: genuinely different control geometry
#
#   anova(m_shared_b, m_separate) -- significant p-value means the
#   exponents genuinely differ; not significant is consistent with (but
#   does not prove) a shared control.
#
# Inputs:
#   03_docs/metadata/ssn703_gaugings_prepped.csv
#
# Outputs:
#   02_processing/plots/ssn703_rc1_rc2_exponent_test.pdf
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


# -----------------------------------------------------------------------------
# 1. Load and filter gaugings
# -----------------------------------------------------------------------------
# Restrict to RC1/RC2, stage_status == "ok" (excludes stage_missing/suspect),
# and Final_rating_curve == "Y" per 06's own guidance for curve fitting work.

gaugings <- read_csv(file.path(meta_dir, "ssn703_gaugings_prepped.csv"),
                      show_col_types = FALSE)

fit_data <- gaugings |>
  filter(
    rating_curve_period %in% c("RC1", "RC2"),
    stage_status == "ok",
    Final_rating_curve == "Y",
    !is.na(Stage_avg_corrected), !is.na(Q_meas)
  ) |>
  mutate(rating_curve_period = factor(rating_curve_period, levels = c("RC1", "RC2")))

message("Gaugings used for fitting: ")
fit_data |> count(rating_curve_period) |> print()

if (any(fit_data |> count(rating_curve_period) |> pull(n) < 8)) {
  warning("One or both periods have very few gaugings (<8) -- exponent fit may be unstable")
}


# -----------------------------------------------------------------------------
# 2. Fit models
# -----------------------------------------------------------------------------
# Starting values: h0 set just below the minimum observed stage per period,
# a and b at generic starting guesses -- adjust if convergence fails.
#
# NOTE: indexed-vector parameters (a[rating_curve_period]) are a base-R nls()
# trick that nlsLM() does not reliably support -- it uses a different
# (Levenberg-Marquardt) backend that can miscount parameters when they're
# expressed as factor-indexed vectors, producing a hessian dimension
# mismatch. Using explicit ifelse()-based scalar parameters per group avoids
# this entirely and is the more robust pattern for nlsLM specifically.

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

message("\n--- ANOVA: shared b vs separate b ---")
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
# 4. If shared-b model is not rejected, extract the implied datum offset
# -----------------------------------------------------------------------------

h0_coefs <- coef(m_shared_b)[c("h01", "h02")]
offset_estimate <- h0_coefs["h02"] - h0_coefs["h01"]

message("\nFitted h0 (effective stage of zero flow) per period:")
message("  RC1 (h01): ", round(h0_coefs["h01"], 2))
message("  RC2 (h02): ", round(h0_coefs["h02"], 2))
message("\nImplied datum offset (h0_RC2 - h0_RC1): ", round(offset_estimate, 2), " cm")


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
set.seed(703)  # reproducibility
boot_result <- boot(fit_data, boot_offset, R = 1000)

n_failed <- sum(is.na(boot_result$t))
message("Bootstrap replicates failed to converge: ", n_failed, " of 1000")

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
# 6. Diagnostic plot -- fitted curves over the data
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
    title    = "SSN703 -- RC1 vs RC2, shared-exponent model fit",
    subtitle = paste0("Shared b = ", round(coef(m_shared_b)["b"], 3),
                      " | Implied offset (h0_RC2 - h0_RC1) = ", round(offset_estimate, 2), " cm",
                      " | ANOVA p = ", round(p_value, 4)),
    x        = "Stage corrected (cm)",
    y        = "Discharge (m³/s)",
    colour   = "RC period",
    caption  = "Points = observed gaugings (post RC1-tail correction) | Lines = shared-b model fit\nGenerated by 08_rc1_rc2_exponent_test.R"
  ) +
  theme_bw() +
  theme(legend.position = "bottom", panel.grid.minor = element_blank())

out_path <- file.path(plot_dir, "ssn703_rc1_rc2_exponent_test.pdf")
pdf(out_path, width = 10, height = 7)
print(p)
dev.off()

message("\nSaved: ", out_path)
message("\nDone. Review the ANOVA result and diagnostic plot before drawing conclusions.")
message("Remember: 'not significant' supports but does not prove shared control --")
message("a real hydrological change at either site during the fitting period could")
message("also produce this result. Cross-check against field notes if the result")
message("surprises you.")
