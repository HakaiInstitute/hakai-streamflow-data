# =============================================================================
# prep_gaugings.R -- build the SSN703 gauging table for rating-curve work
# =============================================================================
# Run from the PROJECT ROOT:
#   source("02_processing/scripts/rating_curve/prep_gaugings.R")
#
# Merges the old 06_gauging_table_prep.R + 07a_recover_historical_stage_rc1.R +
# 07b_Exentd_RC2_Cal.R into one script with explicit stages.
#
# Stages:
#   1. Load raw gaugings, parse datetime, assign rating_curve_period
#   2. DATUM (offset fix) -- see note below
#   3. stage_status / Old_New
#   3b. RC3 stage reassignment (PLS4) -- 2021-09-02 onward (see note below)
#   4. RC1 tail recovery (2018-09-14 -> 2019-02-09): use MK's own original
#      ssn703_b-based Stage_avg for those events (was 07a)
#   5. RC2 refit-calibration extension: the Sept 2018-Feb 2019 events at their
#      RAW sensor-C stage, as an optional second output (was 07b)
#   6. Write ssn703_gaugings_prepped.csv (+ dated backup) + overview plot
#
# ---------------------------------------------------------------------------
# RC2/RC3 BOUNDARY (see 03_docs/decisions/ssn703_pls3_pls4_rc_boundary.md):
#
# The gauging database kept populating Stage_avg from ssn703_c (PLS3) well
# after ssn703_d (PLS4) was installed (2021-09-02) and had already become the
# sensor the field dosing hardware actually referenced (confirmed via
# DoseEvent StreamHeightRelease matching across 4 .dat exports spanning
# 2020-2025 -- see pls3_pls4_dose_comparison.R / pls3_pls4_gauging_comparison.R
# and 04_outputs/pls3_pls4_comparison/). There is no evidence of a genuine
# database fix at 2023-09-15 -- that date is just ssn703_c's physical removal
# (sensor_registry.csv date_end). 03_docs/instrument_history.csv already
# recorded ssn703_d as `primary_during_overlap` from 2021-09-02 onward
# (ssn703_c marked "failing"/non-primary for that same window) -- this was
# just never wired into the rating_curve_period boundary here. Fixed by
# reassigning every RC3 gauging's Stage_avg_corrected from the continuous
# PLS4_Lvl series (stage 3b) and moving the RC2/RC3 cut to ssn703_d's
# registry install date instead of ssn703_c's removal date.
# ---------------------------------------------------------------------------
#
# ---------------------------------------------------------------------------
# DATUM (offset fix -- see memory/mk-rc1-stage-provenance.md and
# rating_curve/recreate_mk_curve.R Part B):
#
# The old 06 added +2 cm to every pre-2017-11-13 ("ssn703_a era") gauging to
# put it "onto the ssn703_b datum". That is backwards. MK's reference datum
# IS ssn703_a, and her gauging-table Stage_avg through 2018-09-14 already
# carries her adjusted stage on that datum. So:
#   - RC1 body (through 2018-09-14): Stage_avg_corrected = Stage_avg,
#     no offset. stage_source = "mk_datum".
#   - RC1 tail (2018-09-14 -> 2019-02-09): stage recovered from MK's own
#     original table (already her adjusted, ssn703_b-derived, on the a datum).
#     stage_source = "historical_ssn703b_recovered".
#   - RC2 / RC3: separate locations, their own periods, no offset.
#
# The ~-1.9 cm ssn703_b correction lives in the CONTINUOUS stage series
# (stage_qc/run_stage_qc_ssn703.R via offsets.csv), not here -- this table
# already carries MK's adjustment.
# ---------------------------------------------------------------------------

library(tidyverse)
library(lubridate)

source("02_processing/scripts/rating_curve/rating_curve_functions.R")

META_DIR <- "03_docs/metadata"
PLOT_DIR <- "02_processing/plots"
dir.create(PLOT_DIR, showWarnings = FALSE, recursive = TRUE)

TZ <- "Etc/GMT+8"   # PST

# Period boundaries (unchanged from 06) -- based on the actual stage SOURCE
SSN703_B_INSTALL <- as.POSIXct("2017-11-13 14:00:00", tz = TZ)
RC1_END          <- as.POSIXct("2018-09-14 20:00:00", tz = TZ)  # ssn703_c install
SSN703_C_STAGE_BAD <- as.POSIXct("2023-06-25 17:00:00", tz = TZ)  # ssn703_c's OWN
  # stage record goes bad -- informational only (bad_data_start in
  # sensor_registry.csv), no longer drives rating_curve_period; see below.

# RC2/RC3 boundary: read from sensor_registry.csv (ssn703_d's install date)
# rather than a second hardcoded literal, so this can't drift independently
# of the registry. Previously this was a hardcoded SUSPECT_END of
# 2023-09-15 (ssn703_c's removal date) -- see the RC2/RC3 BOUNDARY note above
# for why that was wrong.
registry <- read_csv(file.path(META_DIR, "sensor_registry.csv"), show_col_types = FALSE)
RC2_RC3_BOUNDARY <- registry |>
  filter(sensor_id == "ssn703_d") |>
  pull(date_start) |>
  as.POSIXct(tz = "UTC", format = "%Y-%m-%dT%H:%M:%SZ")
stopifnot(length(RC2_RC3_BOUNDARY) == 1, !is.na(RC2_RC3_BOUNDARY))

# Kept only to compute rating_curve_period_old (for the reassignment diff
# export in stage 3b) -- NOT used for the live boundary above.
OLD_RC2_END     <- as.POSIXct("2023-06-25 17:00:00", tz = TZ)
OLD_SUSPECT_END <- as.POSIXct("2023-09-15 00:00:00", tz = TZ)

RC1_TAIL_START <- RC1_END
RC1_TAIL_END   <- as.POSIXct("2019-02-09 00:00:00", tz = TZ)    # v4 pipeline cutoff

Q_MATCH_TOL_M3S <- 0.05   # discharge agreement required to trust a nearest-time match
TIME_MATCH_TOL_MIN <- 10
PLS4_MATCH_MAX_GAP_S <- 200   # nearest-5-min match tolerance for stage 3b


# -----------------------------------------------------------------------------
# 1. Load raw gaugings, parse datetime, assign rating_curve_period
# -----------------------------------------------------------------------------
gaugings <- read_csv(file.path(META_DIR, "ssn703_gaugings_raw.csv"), show_col_types = FALSE) |>
  mutate(datetime = mdy_hms(paste(Date, Start_time), tz = TZ))

n_failed <- sum(is.na(gaugings$datetime))
if (n_failed > 0) warning(n_failed, " rows failed datetime parsing -- check Date/Start_time")

gaugings <- gaugings |>
  mutate(
    # Kept for the stage-3b reassignment-diff export only.
    rating_curve_period_old = case_when(
      datetime < RC1_END         ~ "RC1",
      datetime < OLD_RC2_END     ~ "RC2",
      datetime < OLD_SUSPECT_END ~ "stage_suspect",
      TRUE                       ~ "RC3"
    ),
    rating_curve_period = case_when(
      datetime < RC1_END          ~ "RC1",
      datetime < RC2_RC3_BOUNDARY ~ "RC2",
      TRUE                        ~ "RC3"
    )
  )

message("Gaugings per period (pre RC1-tail recovery):")
gaugings |> count(rating_curve_period) |> print()


# -----------------------------------------------------------------------------
# 2. DATUM -- no offset. Stage_avg_corrected = Stage_avg (MK datum).
# -----------------------------------------------------------------------------
gaugings <- gaugings |>
  mutate(
    Stage_avg_corrected = Stage_avg,
    stage_source = case_when(
      is.na(Stage_avg)              ~ "no_stage",
      rating_curve_period == "RC1"  ~ "mk_datum",
      TRUE                          ~ "raw"
    )
  )


# -----------------------------------------------------------------------------
# 3. stage_status / Old_New
# -----------------------------------------------------------------------------
gaugings <- gaugings |>
  mutate(
    Old_New = if_else(rating_curve_period == "RC1", "Old", "New"),
    stage_status = case_when(
      is.na(Stage_avg) ~ "stage_missing",
      TRUE              ~ "ok"
    )
  )


# -----------------------------------------------------------------------------
# 3b. RC3 stage reassignment (PLS4) -- 2021-09-02 onward
# -----------------------------------------------------------------------------
# See the RC2/RC3 BOUNDARY note at the top of this file. Every gauging now
# classified RC3 (i.e. datetime >= RC2_RC3_BOUNDARY) gets Stage_avg_corrected
# rebuilt from the continuous PLS4_Lvl series, nearest-5-min-matched to the
# gauging's datetime. `datetime` was parsed directly from Date/Start_time in
# stage 1 above and is already correctly local-PST-tagged -- there is no CSV
# round-trip in between, so no with_tz()/force_tz() conversion is needed here
# (that distinction mattered, and bit us, in the exploratory comparison
# scripts, which DID round-trip through a CSV -- see
# 03_docs/decisions/ssn703_pls3_pls4_rc_boundary.md).
# The raw Stage_avg column is left untouched for provenance, same pattern as
# the RC1-tail recovery below -- only Stage_avg_corrected changes.

read_pls4_series <- function(path = "01_raw/SSN703/ssn703_d.csv") {
  nms <- names(read_csv(path, skip = 3, n_max = 0, show_col_types = FALSE))
  read_csv(path, skip = 4, col_names = nms, show_col_types = FALSE,
           col_types = cols(.default = "c")) |>
    transmute(
      measurementTime = as.POSIXct(measurementTime, format = "%Y-%m-%d %H:%M:%S", tz = TZ),
      value = as.numeric(.data[["PLS4_Lvl"]])
    ) |>
    filter(!is.na(measurementTime)) |>
    arrange(measurementTime)
}

nearest_pls4 <- function(series, target, max_gap_s = PLS4_MATCH_MAX_GAP_S) {
  if (is.na(target) || nrow(series) == 0) return(c(value = NA_real_, gap_s = NA_real_))
  idx <- findInterval(target, series$measurementTime)
  cands <- unique(pmin(pmax(c(idx, idx + 1), 1), nrow(series)))
  gaps <- abs(as.numeric(difftime(series$measurementTime[cands], target, units = "secs")))
  best <- cands[which.min(gaps)]
  gap  <- gaps[which.min(gaps)]
  if (gap > max_gap_s) return(c(value = NA_real_, gap_s = gap))
  c(value = series$value[best], gap_s = gap)
}

pls4_series <- read_pls4_series()

# Snapshot pre-reassignment values for the diff export, before mutating.
gaugings <- gaugings |>
  mutate(
    Stage_avg_corrected_old = Stage_avg_corrected,
    stage_source_old        = stage_source,
    stage_status_old        = stage_status
  )

rc3_idx <- which(gaugings$rating_curve_period == "RC3")
if (length(rc3_idx) > 0) {
  pls4_match <- vapply(gaugings$datetime[rc3_idx],
                        function(t) nearest_pls4(pls4_series, t), numeric(2))
  pls4_stage_cm <- unname(pls4_match["value", ]) * 100
  n_failed <- sum(is.na(pls4_stage_cm))
  if (n_failed > 0) {
    warning(n_failed, " of ", length(rc3_idx),
            " RC3 gaugings had no PLS4 match within ", PLS4_MATCH_MAX_GAP_S,
            "s -- Stage_avg_corrected left unchanged for those rows")
  }

  gaugings$Stage_avg_corrected[rc3_idx] <- if_else(
    !is.na(pls4_stage_cm), pls4_stage_cm, gaugings$Stage_avg_corrected[rc3_idx]
  )
  gaugings$stage_source[rc3_idx] <- if_else(
    !is.na(pls4_stage_cm), "pls4_reassigned", "pls4_match_failed"
  )
  gaugings$stage_status[rc3_idx] <- if_else(
    !is.na(pls4_stage_cm), "ok", "stage_missing"
  )

  message("RC3 gaugings reassigned to PLS4 stage: ",
          sum(!is.na(pls4_stage_cm)), " of ", length(rc3_idx),
          " (", n_failed, " failed match)")
}


# -----------------------------------------------------------------------------
# 4. RC1 tail recovery (2018-09-14 -> 2019-02-09) -- from MK's original table
# -----------------------------------------------------------------------------
# Those events currently carry ssn703_c stage (the autosalt DB logged whatever
# sensor was active). RC1's curve was fit by MK on ssn703_b (loc_1) stage --
# recover her own recorded Stage_avg for that window. It is already her
# adjusted value, on the ssn703_a datum, so no further correction is applied.
# No historical discharge is altered -- only the fitting table.

mk_orig <- read_csv(
  file.path(META_DIR, "MK_original_gauging_table_703.csv"),
  col_types = cols(Date = col_character(), Start_time = col_character(), .default = col_guess())
) |>
  mutate(datetime_pst = ymd_hm(paste(Date, Start_time), tz = TZ)) |>
  filter(!is.na(datetime_pst),
         datetime_pst >= RC1_TAIL_START, datetime_pst <= RC1_TAIL_END) |>
  select(datetime_pst, Stage_avg_hist = Stage_avg, Q_meas_hist = Q_meas)

message("MK original-table rows in the RC1 tail window: ", nrow(mk_orig))

if (nrow(mk_orig) > 0) {
  # `datetime` here is parsed fresh in PST (stage 1), so it matches
  # mk_orig$datetime_pst directly -- no tz conversion (this is the step the
  # old 07a got wrong when it assumed the table's datetime was UTC).
  tail_events <- gaugings |>
    mutate(row_id = row_number()) |>
    filter(datetime >= RC1_TAIL_START, datetime <= RC1_TAIL_END)

  matched <- tail_events |>
    rowwise() |>
    mutate(
      .mi = which.min(abs(mk_orig$datetime_pst - datetime)),
      .dt = as.numeric(abs(mk_orig$datetime_pst[.mi] - datetime), units = "mins"),
      Stage_avg_hist = if_else(.dt <= TIME_MATCH_TOL_MIN, mk_orig$Stage_avg_hist[.mi], NA_real_),
      Q_meas_hist    = if_else(.dt <= TIME_MATCH_TOL_MIN, mk_orig$Q_meas_hist[.mi],   NA_real_),
      q_diff         = abs(Q_meas - Q_meas_hist),
      safe           = !is.na(Stage_avg_hist) & (is.na(q_diff) | q_diff <= Q_MATCH_TOL_M3S)
    ) |>
    ungroup()

  n_qbad <- sum(matched$safe & !is.na(matched$q_diff) & matched$q_diff > Q_MATCH_TOL_M3S, na.rm = TRUE)
  if (n_qbad > 0) warning(n_qbad, " RC1-tail matches have a Q_meas mismatch > ",
                          Q_MATCH_TOL_M3S, " m3/s -- review before trusting the stage match")

  gaugings <- gaugings |>
    mutate(row_id = row_number()) |>
    left_join(matched |> select(row_id, Stage_avg_hist, safe), by = "row_id") |>
    mutate(
      Stage_avg_corrected = if_else(coalesce(safe, FALSE), Stage_avg_hist, Stage_avg_corrected),
      stage_source        = if_else(coalesce(safe, FALSE), "historical_ssn703b_recovered", stage_source),
      rating_curve_period = if_else(coalesce(safe, FALSE), "RC1", rating_curve_period),
      Old_New             = if_else(coalesce(safe, FALSE), "Old", Old_New),
      stage_status        = if_else(coalesce(safe, FALSE), "ok", stage_status)
    ) |>
    select(-row_id, -Stage_avg_hist, -safe)

  message("RC1-tail rows recovered from MK's original table: ",
          sum(gaugings$stage_source == "historical_ssn703b_recovered", na.rm = TRUE))
}


# -----------------------------------------------------------------------------
# 4b. Reconcile the per-gauging approval file (RC1/RC2/RC3 accept-reject flag)
# -----------------------------------------------------------------------------
# ssn703_rating_approvals.csv is the manual curve-fitting accept/reject flag.
# Emily edits `final_rating` there after reviewing the gaugings in
# fit_rating_curves.Rmd. This overwrites Final_rating_curve with the resolved
# value and keeps MK's original as Final_rating_curve_mk. Seed state == the
# pre-gating fit set, so a first run changes nothing.
source("02_processing/scripts/rating_curve/rating_approvals.R")
gaugings <- sync_rating_approvals(gaugings, META_DIR)


# -----------------------------------------------------------------------------
# 5. Write the prepped table (+ dated backup)
# -----------------------------------------------------------------------------
keep_cols <- c(
  "EventID", "MID", "SiteID", "Method", "Event_no",
  "datetime", "Date", "Start_time", "WY",
  "rating_curve_period", "Old_New", "Final_rating_curve", "Final_rating_curve_mk",
  "Stage_avg", "Stage_avg_corrected", "Stage_stdv", "Stage_delta",
  "stage_source", "stage_status",
  "Q_meas", "Q_rel_unc", "Mixing", "ecb", "Comments"
)
gaugings_out <- gaugings |> select(any_of(keep_cols)) |> arrange(datetime)

prepped_path <- file.path(META_DIR, "ssn703_gaugings_prepped.csv")
if (file.exists(prepped_path)) {
  file.copy(prepped_path,
            file.path(META_DIR, paste0("ssn703_gaugings_prepped_prev_", Sys.Date(), ".csv")),
            overwrite = TRUE)
}
write_csv(gaugings_out, prepped_path)
message("\nSaved: ", prepped_path)

message("\nstage_source breakdown:")
gaugings_out |> count(rating_curve_period, stage_source) |> print()


# -----------------------------------------------------------------------------
# 5b. RC3 stage-reassignment diff export (for review)
# -----------------------------------------------------------------------------
# Every gauging whose rating_curve_period changed to RC3 under the new
# (2021-09-02) boundary but was NOT already RC3 under the old (2023-09-15)
# boundary -- i.e. the actual reassignment population, not just the events
# that were already correctly RC3. See
# 03_docs/decisions/ssn703_pls3_pls4_rc_boundary.md.
reassignment_diff <- gaugings |>
  filter(rating_curve_period == "RC3", rating_curve_period_old != "RC3") |>
  transmute(
    EventID, MID, datetime, WY, Method,
    rating_curve_period_old, rating_curve_period,
    stage_source_old, stage_source,
    stage_status_old, stage_status,
    Stage_avg_raw = Stage_avg,
    Stage_avg_corrected_old, Stage_avg_corrected,
    stage_change_cm = Stage_avg_corrected - Stage_avg_corrected_old,
    Final_rating_curve   # carried-over approval flag -- see decision log: needs re-review
  ) |>
  arrange(datetime)

diff_path <- file.path(META_DIR, "ssn703_pls4_reassignment_diff.csv")
write_csv(reassignment_diff, diff_path)
message("\nSaved: ", diff_path, " (", nrow(reassignment_diff), " gaugings reassigned RC2/stage_suspect -> RC3)")
message("Review these -- their Final_rating_curve approval flag carried over from the old,")
message("PLS3-based classification and has NOT been re-evaluated for the new PLS4 stage.")


# -----------------------------------------------------------------------------
# 6. RC2 refit-calibration extension (optional -- was 07b)
# -----------------------------------------------------------------------------
# RC2 was calibrated on Feb 2019+ gaugings only. Real ssn703_c stage exists
# for Sept 2018-Feb 2019 too -- those events' ORIGINAL sensor-C readings live
# in the raw Stage_avg column (untouched by stage 4, which only moved
# Stage_avg_corrected). Build a combined RC2 calibration set using the raw
# sensor-C stage for the extension events. This does NOT refit the curve --
# feed ssn703_rc2_refit_calibration.csv (column stage_for_rc2) into
# fit_rating_curves.Rmd's RC2 block if you want to use it.

rc2_existing <- gaugings_out |>
  filter(rating_curve_period == "RC2", stage_status == "ok", !is.na(Q_meas)) |>
  transmute(EventID, MID, Event_no, datetime, WY, Method, Final_rating_curve,
            stage_for_rc2 = Stage_avg_corrected, Q_meas, Q_rel_unc,
            calibration_source = "existing_rc2")

rc2_extension <- gaugings_out |>
  filter(stage_source == "historical_ssn703b_recovered", !is.na(Q_meas), !is.na(Stage_avg)) |>
  transmute(EventID, MID, Event_no, datetime, WY, Method, Final_rating_curve,
            stage_for_rc2 = Stage_avg,   # ORIGINAL sensor-C reading, not corrected
            Q_meas, Q_rel_unc,
            calibration_source = "sept2018_feb2019_sensorc_extension")

rc2_refit <- bind_rows(rc2_existing, rc2_extension) |> arrange(datetime)
write_csv(rc2_refit, file.path(META_DIR, "ssn703_rc2_refit_calibration.csv"))
message("\nSaved: ", file.path(META_DIR, "ssn703_rc2_refit_calibration.csv"),
        "  (", nrow(rc2_existing), " existing + ", nrow(rc2_extension), " extension)")


# -----------------------------------------------------------------------------
# 7. Overview plot
# -----------------------------------------------------------------------------
rc_colours <- c(RC1 = "#E41A1C", RC2 = "#4DAF4A", RC3 = "#984EA3")

p <- gaugings_out |>
  filter(stage_status == "ok", !is.na(Q_meas)) |>
  ggplot(aes(Stage_avg_corrected, Q_meas, colour = rating_curve_period, shape = Method)) +
  geom_point(size = 2, alpha = 0.85, na.rm = TRUE) +
  scale_colour_manual(values = rc_colours) +
  labs(title = "SSN703 -- prepped gaugings, stage vs discharge",
       subtitle = "Stage in cm on the ssn703_a (MK) datum -- no offset added; RC1 tail recovered from MK's original table",
       x = "Stage corrected (cm)", y = "Discharge (m3/s)",
       colour = "RC period", caption = "Generated by rating_curve/prep_gaugings.R") +
  theme_bw() + theme(legend.position = "bottom", panel.grid.minor = element_blank())

ggsave(file.path(PLOT_DIR, "ssn703_gauging_overview.pdf"), p, width = 10, height = 7)
message("\nSaved: ", file.path(PLOT_DIR, "ssn703_gauging_overview.pdf"))
message("\nNext: explore_gaugings.R, then fit_rating_curves.Rmd")
message("Review gaugings in fit_rating_curves.Rmd's 'Gauging review' section, set")
message("  final_rating (Y/N) in 03_docs/metadata/ssn703_rating_approvals.csv, re-run this.")
