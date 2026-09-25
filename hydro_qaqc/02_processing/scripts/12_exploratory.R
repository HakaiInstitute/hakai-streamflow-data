# =============================================================================
# SSN703 -- Exploratory: can loc_2 (RC2) and loc_3 (RC3) gaugings be combined
# onto a single curve via a stage offset?
# =============================================================================
# Purpose:
#   RC3 (loc_3, ssn703_d) is sparse (~18 gaugings). RC2 (loc_2, ssn703_c) is
#   well-populated. This script tests whether a vertical stage offset can
#   reconcile the two gauging sets onto a single stage-discharge relationship.
#
#   HYPOTHESIS (expected to FAIL based on hydraulic reasoning):
#     loc_2 and loc_3 are different physical locations with different channel
#     geometry. A simple offset should NOT fully reconcile them. But if the
#     geometry happens to be similar, the gaugings may align well enough to
#     justify a combined curve with documented higher uncertainty.
#
#   DIAGNOSTIC LOGIC:
#     - If a single offset aligns gaugings across ALL flows -> offset defensible
#     - If gaugings align at some flows but diverge at others -> geometry differs,
#       keep separate curves
#     - If no offset aligns them -> definitely separate curves
#
# Inputs:
#   03_docs/metadata/ssn703_gaugings_prepped.csv
#
# Outputs:
#   Diagnostic plots (interactive / on screen)
#   Console summary of best-fit offset and residual behaviour
#
# NOTE: This is exploratory only. It does NOT modify any production curve.
# =============================================================================

library(tidyverse)

select <- dplyr::select

# -----------------------------------------------------------------------------
# 1. Load gaugings and separate by location / rating
# -----------------------------------------------------------------------------

gaugings <- read_csv("03_docs/metadata/ssn703_gaugings_prepped.csv",
                     show_col_types = FALSE)

# Inspect available columns -- adjust names below if needed
message("Gauging table columns:")
print(names(gaugings))

# EXPECTED columns (adjust to match your actual prepped file):
#   Stage_avg        -- stage in cm
#   Q                -- measured discharge (m3/s)
#   Final_rating_curve or rating -- which rating/location the gauging belongs to
#   site_id or location -- sensor/location identifier

# Filter to the two ratings of interest.
# ADJUST these filter values to match how loc_2 and loc_3 gaugings are labelled
# in your prepped file (could be rating number, RC label, or sensor id)
loc2_gaugings <- gaugings |>
  filter(rating_curve_period == "RC2",
         Final_rating_curve == "Y") |>
  filter(!is.na(Stage_avg), !is.na(Q_meas), Q_meas > 0)

loc3_gaugings <- gaugings |>
  filter(rating_curve_period == "RC3",
         Final_rating_curve == "Y") |>
  filter(!is.na(Stage_avg), !is.na(Q_meas), Q_meas > 0)

message("\nloc_2 (RC2) gaugings: ", nrow(loc2_gaugings))
message("loc_3 (RC3) gaugings: ", nrow(loc3_gaugings))

# -----------------------------------------------------------------------------
# 2. Visual baseline -- plot both sets without any offset
# -----------------------------------------------------------------------------

combined_raw <- bind_rows(
  loc2_gaugings |> mutate(loc = "loc_2 (RC2)"),
  loc3_gaugings |> mutate(loc = "loc_3 (RC3)")
)

p_baseline <- ggplot(combined_raw, aes(x = Stage_avg, y = Q_meas, colour = loc)) +
  geom_point(size = 2, alpha = 0.7) +
  scale_colour_manual(values = c("loc_2 (RC2)" = "#377EB8",
                                 "loc_3 (RC3)" = "#E41A1C")) +
  labs(title = "SSN703 -- loc_2 vs loc_3 gaugings, NO offset",
       subtitle = "If these don't overlap, a stage offset may or may not reconcile them",
       x = "Stage (cm)", y = "Discharge (m³/s)", colour = NULL) +
  theme_bw()

print(p_baseline)

# -----------------------------------------------------------------------------
# 3. Grid search for best stage offset
# -----------------------------------------------------------------------------
# Apply a trial offset to loc_3 stage, then for each loc_3 gauging find the
# discharge predicted by a smooth fit through loc_2 gaugings at the offset
# stage. Sum of squared log-residuals measures alignment. Log space because
# discharge spans orders of magnitude.

# Fit a smooth reference relationship through loc_2 gaugings (power law in log space)
loc2_fit <- lm(log(Q_meas) ~ log(Stage_avg), data = loc2_gaugings)

# Function: given an offset, compute alignment of loc_3 onto loc_2 curve
loc2_stage_min <- min(loc2_gaugings$Stage_avg, na.rm = TRUE)
loc2_stage_max <- max(loc2_gaugings$Stage_avg, na.rm = TRUE)

offset_misfit <- function(offset) {
  shifted <- loc3_gaugings |>
    mutate(
      stage_shifted = Stage_avg + offset,
      # only evaluate where shifted stage is within loc_2 gauged range
      in_range = stage_shifted >= loc2_stage_min &
        stage_shifted <= loc2_stage_max
    ) |>
    filter(in_range, stage_shifted > 0)
  
  if (nrow(shifted) < 3) return(NA_real_)
  
  pred_logQ <- predict(loc2_fit,
                       newdata = data.frame(Stage_avg = shifted$stage_shifted))
  resid <- log(shifted$Q_meas) - pred_logQ
  sqrt(mean(resid^2))   # RMSE in log space
}

# Search offsets from -30cm to +30cm
offset_grid <- seq(-30, 30, by = 0.5)
misfit <- map_dbl(offset_grid, offset_misfit)

offset_results <- tibble(offset = offset_grid, rmse_log = misfit) |>
  filter(!is.na(rmse_log))

best_offset <- offset_results |> slice_min(rmse_log, n = 1)

message("\n=== Offset grid search ===")
message("Best offset: ", best_offset$offset, " cm")
message("RMSE (log space) at best offset: ", round(best_offset$rmse_log, 4))
message("RMSE at zero offset: ",
        round(offset_results |> filter(offset == 0) |> pull(rmse_log), 4))

p_grid <- ggplot(offset_results, aes(x = offset, y = rmse_log)) +
  geom_line() +
  geom_vline(xintercept = best_offset$offset, colour = "red", linetype = "dashed") +
  annotate("text", x = best_offset$offset, y = max(offset_results$rmse_log),
           label = paste0("best = ", best_offset$offset, " cm"),
           hjust = -0.1, colour = "red") +
  labs(title = "SSN703 -- offset grid search (loc_3 onto loc_2)",
       subtitle = "Lower RMSE = better alignment. A clear deep minimum suggests an offset works.",
       x = "Stage offset applied to loc_3 (cm)", y = "RMSE (log discharge)") +
  theme_bw()

print(p_grid)

# -----------------------------------------------------------------------------
# 4. Plot gaugings with best offset applied
# -----------------------------------------------------------------------------

combined_shifted <- bind_rows(
  loc2_gaugings |> mutate(loc = "loc_2 (RC2)", stage_plot = Stage_avg),
  loc3_gaugings |> mutate(loc = "loc_3 (RC3) + offset",
                          stage_plot = Stage_avg + best_offset$offset)
)

p_shifted <- ggplot(combined_shifted, aes(x = stage_plot, y = Q_meas, colour = loc)) +
  geom_point(size = 2, alpha = 0.7) +
  scale_colour_manual(values = c("loc_2 (RC2)" = "#377EB8",
                                 "loc_3 (RC3) + offset" = "#E41A1C")) +
  labs(title = paste0("SSN703 -- gaugings with best offset (",
                      best_offset$offset, " cm) applied to loc_3"),
       subtitle = "If red points now sit ON the blue cloud across ALL flows -> offset defensible\nIf they align at some flows but not others -> geometry differs, keep separate",
       x = "Stage (cm, loc_3 offset-adjusted)", y = "Discharge (m³/s)",
       colour = NULL) +
  theme_bw()

print(p_shifted)

# -----------------------------------------------------------------------------
# 5. Residual diagnostic -- is misfit flow-dependent?
# -----------------------------------------------------------------------------
# The critical test: even at the best offset, do residuals vary systematically
# with stage? If yes, the two locations have different curve SHAPES (geometry
# differs) and no single offset can reconcile them.

loc3_resid <- loc3_gaugings |>
  mutate(
    stage_shifted = Stage_avg + best_offset$offset,
    pred_logQ = predict(loc2_fit,
                        newdata = data.frame(Stage_avg = stage_shifted)),
    resid_log = log(Q_meas) - pred_logQ
  )

p_resid <- ggplot(loc3_resid, aes(x = stage_shifted, y = resid_log)) +
  geom_point(size = 2, colour = "#E41A1C") +
  geom_hline(yintercept = 0, colour = "black") +
  geom_smooth(method = "loess", se = TRUE, colour = "grey40", linewidth = 0.6) +
  labs(title = "SSN703 -- loc_3 residuals vs stage at best offset",
       subtitle = "FLAT around zero = offset works | TREND with stage = geometry differs, keep separate curves",
       x = "Stage (cm, offset-adjusted)", y = "Log residual (loc_3 obs - loc_2 curve)") +
  theme_bw()

print(p_resid)

# -----------------------------------------------------------------------------
# 6. Verdict guidance
# -----------------------------------------------------------------------------

message("\n=== INTERPRETATION GUIDE ===")
message("1. Look at p_grid: is there a clear, deep minimum? ")
message("   - Sharp minimum  -> an offset meaningfully improves alignment")
message("   - Flat/shallow   -> offset doesn't help much; locations differ")
message("2. Look at p_shifted: do red points sit on blue across ALL flows?")
message("3. Look at p_resid: is the loess line FLAT around zero?")
message("   - Flat    -> offset is defensible; consider combined curve")
message("   - Trended -> curve SHAPES differ; offset cannot reconcile; keep separate")
message("\nReminder: even if an offset works statistically, document the physical")
message("justification. Different locations with similar geometry CAN share a curve,")
message("but the reasoning must be explicit for the dataset record.")