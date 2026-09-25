# =============================================================================
# SSN703 - Script 07b: Build Extended RC2 Calibration Dataset
# =============================================================================
# Purpose:
#   RC2 (ssn703_c) was originally calibrated only on gaugings from Feb 2019
#   onward. But real ssn703_c stage exists for Sept 2018-Feb 2019 too --
#   those 28 events' ORIGINAL sensor-C readings were never deleted, just
#   set aside in favour of the historical ssn703_b values needed for RC1's
#   calibration provenance (07_recover_historical_stage_rc1_tail.R only
#   modified Stage_avg_corrected/stage_source/rating_curve_period/stage_status
#   -- the raw Stage_avg column is untouched).
#
#   This script builds a combined RC2 calibration set: the existing Feb
#   2019+ RC2 gaugings, plus the Sept 2018-Feb 2019 events using their real
#   original sensor-C stage (not the recovered ssn703_b values, and not the
#   RC1 label those events now correctly carry for calibration-provenance
#   purposes -- this is a DIFFERENT use of the same underlying raw data).
#
#   Output is a calibration dataset ready to feed into your normal RC2
#   curve-fitting process (LOESS + power law extrapolation) -- this script
#   does not refit the curve itself, just prepares the correct input data.
#
# Inputs:
#   03_docs/metadata/ssn703_gaugings_prepped.csv  (post 07's correction, promoted)
#
# Outputs:
#   03_docs/metadata/ssn703_rc2_refit_calibration.csv
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)

meta_dir <- "03_docs/metadata"

RC2_START <- as.POSIXct("2018-09-14 20:00:00", tz = "Etc/GMT+8")  # matches sensor_registry authoritative_start


# -----------------------------------------------------------------------------
# 1. Load gauging table
# -----------------------------------------------------------------------------

gaugings <- read_csv(file.path(meta_dir, "ssn703_gaugings_prepped.csv"), show_col_types = FALSE)


# -----------------------------------------------------------------------------
# 2. Existing RC2 gaugings (Feb 2019 onward, unchanged)
# -----------------------------------------------------------------------------

rc2_existing <- gaugings |>
  filter(rating_curve_period == "RC2", stage_status == "ok", !is.na(Q_meas)) |>
  mutate(
    stage_for_rc2 = Stage_avg_corrected,
    calibration_source = "existing_rc2"
  )

message("Existing RC2 gaugings: ", nrow(rc2_existing))


# -----------------------------------------------------------------------------
# 3. Sept 2018-Feb 2019 events -- use ORIGINAL sensor-C stage
# -----------------------------------------------------------------------------
# These events currently carry rating_curve_period == "RC1" and
# stage_source == "historical_ssn703b_recovered" (correct for RC1's
# calibration provenance) -- Stage_avg_corrected for these rows now holds
# the recovered ssn703_b value, NOT the sensor-C reading we want here.
# The original sensor-C reading is preserved in the raw Stage_avg column.

rc2_extension <- gaugings |>
  filter(stage_source == "historical_ssn703b_recovered",
         !is.na(Q_meas), !is.na(Stage_avg)) |>
  mutate(
    stage_for_rc2 = Stage_avg,  # ORIGINAL sensor-C value, not Stage_avg_corrected
    calibration_source = "sept2018_feb2019_sensorc_extension"
  )

message("Sept 2018-Feb 2019 extension events (using original sensor-C stage): ",
        nrow(rc2_extension))

if (nrow(rc2_extension) == 0) {
  warning("No extension events found -- check that ssn703_gaugings_prepped.csv is the ",
          "POST-correction, PROMOTED version (should contain 'historical_ssn703b_recovered' rows)")
}


# -----------------------------------------------------------------------------
# 4. Combine
# -----------------------------------------------------------------------------

rc2_refit_calibration <- bind_rows(
  rc2_existing |> select(EventID, MID, datetime, WY, Method, Final_rating_curve,
                          stage_for_rc2, Q_meas, Q_rel_unc, calibration_source),
  rc2_extension |> select(EventID, MID, datetime, WY, Method, Final_rating_curve,
                           stage_for_rc2, Q_meas, Q_rel_unc, calibration_source)
) |>
  arrange(datetime)

message("\nCombined RC2 refit calibration set: ", nrow(rc2_refit_calibration), " gaugings")
message("  From existing RC2 (Feb 2019+): ", sum(rc2_refit_calibration$calibration_source == "existing_rc2"))
message("  From Sept 2018-Feb 2019 extension: ",
        sum(rc2_refit_calibration$calibration_source == "sept2018_feb2019_sensorc_extension"))
message("\nDate range: ", min(rc2_refit_calibration$datetime), " to ", max(rc2_refit_calibration$datetime))
message("Stage range: ", round(min(rc2_refit_calibration$stage_for_rc2), 1), " to ",
        round(max(rc2_refit_calibration$stage_for_rc2), 1), " cm")

write_csv(rc2_refit_calibration, file.path(meta_dir, "ssn703_rc2_refit_calibration.csv"))
message("\nSaved: ", file.path(meta_dir, "ssn703_rc2_refit_calibration.csv"))


# -----------------------------------------------------------------------------
# 5. Quick visual check
# -----------------------------------------------------------------------------

p <- rc2_refit_calibration |>
  ggplot(aes(x = stage_for_rc2, y = Q_meas, colour = calibration_source)) +
  geom_point(size = 2, alpha = 0.8) +
  scale_colour_manual(values = c("existing_rc2" = "#4DAF4A",
                                  "sept2018_feb2019_sensorc_extension" = "#E41A1C")) +
  labs(
    title = "SSN703 -- Extended RC2 calibration dataset",
    subtitle = "Red = Sept 2018-Feb 2019 events, added using their real (original) sensor-C stage",
    x = "Stage, sensor C (cm)", y = "Discharge (m3/s)", colour = NULL,
    caption = "Feed this file into your normal RC2 curve-fitting process (LOESS + power law) in place of the Feb-2019-only set"
  ) +
  theme_bw() + theme(legend.position = "bottom")

print(p)

message("\nDone. Next step: run your normal RC2 curve-fitting process on ",
        "ssn703_rc2_refit_calibration.csv (column 'stage_for_rc2') instead of ",
        "the previous Feb-2019-only gauging set.")