# =============================================================================
# SSN844 Stage QC - Script 06: Gauging Table Preparation
# =============================================================================
# Purpose:
#   Prepare the gauging table for rating curve development by:
#   1. Parsing datetime from Date and Start_time columns
#   2. Assigning rating curve period based on gauging date
#   3. Computing water year
#   4. Flagging gaugings with missing stage
#
# Sensor history (from instrument_history.csv):
#   SSN844: single sensor ssn844 at loc_1 from 2014-09-10 to present
#   No sensor swaps requiring datum correction
#   Supplementary sensor ssn844_sa at loc_2 (gap filling only, not used here)
#
# Previous pipeline (v4) rating periods:
#   Rating 1: 2012-01-01 onward (original)
#   Rating 2: 2017-07-13 onward (sensor replaced, position may have shifted)
#
# RC period for new pipeline:
#   RC1: all gaugings -- single sensor, single location throughout
#        Previous pipeline covered up to 2019-02-09
#        New gaugings (Old_New == "New") extend from 2019-02-09 onward
#
# No datum correction needed -- single sensor location throughout
#
# Stage units:
#   Stage_avg in this table is in cm -- kept as-is for rating curve work
#
# Inputs:
#   03_docs/metadata/ssn844_gaugings_raw.csv
#
# Outputs:
#   03_docs/metadata/ssn844_gaugings_prepped.csv
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)


# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

meta_dir <- "03_docs/metadata"

# Key datetime boundaries
RATING2_START <- as.POSIXct("2017-07-13 00:00:00", tz = "Etc/GMT+8")
OLD_NEW_SPLIT <- as.POSIXct("2019-02-09 00:00:00", tz = "Etc/GMT+8")


# -----------------------------------------------------------------------------
# 1. Load gauging table
# -----------------------------------------------------------------------------

gaugings <- read_csv(
  file.path(meta_dir, "ssn844_gaugings_raw.csv"),
  show_col_types = FALSE
)

message("Loaded ", nrow(gaugings), " gaugings")


# -----------------------------------------------------------------------------
# 2. Parse datetime
# -----------------------------------------------------------------------------

gaugings <- gaugings |>
  mutate(
    datetime = mdy_hms(paste(Date, Start_time), tz = "Etc/GMT+8")
  )

n_failed <- sum(is.na(gaugings$datetime))
if (n_failed > 0) {
  warning(n_failed, " rows failed datetime parsing -- review Date/Start_time columns")
  gaugings |> filter(is.na(datetime)) |> dplyr::select(Date, Start_time) |> print()
}


# -----------------------------------------------------------------------------
# 3. Assign rating curve period
# -----------------------------------------------------------------------------
# SSN844 has a single sensor at a single location throughout.
# All gaugings belong to RC1 -- the diagnostic plot will determine whether
# the previous Rating 2 curve is still valid or a new curve is needed.

gaugings <- gaugings |>
  mutate(
    rating_curve_period = "RC1",
    old_rating = case_when(
      datetime < RATING2_START ~ 1L,
      TRUE                     ~ 2L
    )
  )

message("\nGaugings per old rating:")
gaugings |> count(old_rating) |> print()


# -----------------------------------------------------------------------------
# 4. Update Old_New
# -----------------------------------------------------------------------------
# Recompute from datetime to ensure consistency
# Old = pre 2019-02-09 (covered by previous pipeline)
# New = post 2019-02-09 (not previously incorporated)

gaugings <- gaugings |>
  mutate(
    Old_New = case_when(
      datetime < OLD_NEW_SPLIT ~ "Old",
      TRUE                     ~ "New"
    )
  )

message("\nOld vs New gaugings:")
gaugings |> count(Old_New) |> print()


# -----------------------------------------------------------------------------
# 5. Compute water year
# -----------------------------------------------------------------------------

gaugings <- gaugings |>
  mutate(
    WY = case_when(
      month(datetime) >= 10 ~ paste0(year(datetime), "-", year(datetime) + 1),
      TRUE                  ~ paste0(year(datetime) - 1, "-", year(datetime))
    )
  )


# -----------------------------------------------------------------------------
# 6. Stage source and status
# -----------------------------------------------------------------------------
# Single sensor throughout -- no offset needed

gaugings <- gaugings |>
  mutate(
    stage_source = case_when(
      is.na(Stage_avg) ~ "no_stage",
      TRUE             ~ "raw"
    ),
    stage_status = case_when(
      is.na(Stage_avg) ~ "stage_missing",
      TRUE             ~ "ok"
    ),
    # Stage_avg_corrected = Stage_avg (no correction needed for 844)
    Stage_avg_corrected = Stage_avg
  )

message("\nStage status:")
gaugings |> count(stage_status) |> print()

message("\nStage missing by old rating:")
gaugings |>
  filter(stage_status == "stage_missing") |>
  count(old_rating) |>
  print()


# -----------------------------------------------------------------------------
# 7. Reorder and save
# -----------------------------------------------------------------------------

gaugings_out <- gaugings |>
  dplyr::select(
    EventID, MID, SiteID, Method,
    datetime, Date, Start_time, WY,
    rating_curve_period, old_rating, Old_New, Final_rating_curve,
    Stage_avg, Stage_avg_corrected, Stage_stdv, Stage_delta,
    stage_source, stage_status,
    Q_meas, Q_rel_unc,
    Mixing, ecb, Comments
  ) |>
  arrange(datetime)

write_csv(gaugings_out, file.path(meta_dir, "ssn844_gaugings_prepped.csv"))
message("\nSaved: ", file.path(meta_dir, "ssn844_gaugings_prepped.csv"))

message("\nNext step: run 08_rating_curve_fit_844.Rmd diagnostic section")
message("  to determine whether previous Rating 2 curve is still valid")
