# =============================================================================
# SSN703 Gauging Table - Recover historical ssn703_b stage for RC1 tail
# =============================================================================
# Purpose:
#   For gaugings between 2018-09-14 (ssn703_c physical install) and
#   2019-02-09 (v4 pipeline cutoff), the current gauging table's Stage_avg
#   reflects ssn703_c (loc_2), because the autosalt database associated all
#   salt doses in that window with whichever sensor was logging at the time.
#   However, the RC1 rating curve (and the historical/production discharge
#   record for this window) was fit manually by [colleague] using ssn703_b
#   (loc_1) stage exclusively -- confirmed both by her explicit documentation
#   and by her original gauging table (MK_original_gauging_table_703.csv),
#   which has been matched against the current table and confirms a real,
#   flow-dependent stage difference (not a formatting artifact).
#
#   This script recovers her original ssn703_b stage values for this window
#   and applies them to the current gauging table, so that:
#     1. The gauging table's stage-Q pairing matches what was actually used
#        to produce the historical RC1 discharge record (no invented values)
#     2. rating_curve_period for these events reverts to RC1, correctly
#        reflecting that they were never fit into RC2
#     3. stage_source is flagged distinctly ("historical_ssn703b_recovered")
#        so this correction is traceable and not mistaken for a raw reading
#
#   No historical discharge output is altered by this script -- only the
#   gauging table used for curve fitting going forward. Per team decision,
#   the historical/published discharge record for this window is NOT being
#   retroactively recomputed.
#
# Inputs:
#   03_docs/metadata/ssn703_gaugings_prepped.csv     (or raw -- see note below)
#   03_docs/metadata/MK_original_gauging_table_703.csv
#
# Outputs:
#   03_docs/metadata/ssn703_gaugings_prepped_corrected.csv
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)

meta_dir <- "03_docs/metadata"

RC1_TAIL_START <- as.POSIXct("2018-09-14 20:00:00", tz = "Etc/GMT+8")
RC1_TAIL_END   <- as.POSIXct("2019-02-09 00:00:00", tz = "Etc/GMT+8")


# -----------------------------------------------------------------------------
# 1. Load current gauging table
# -----------------------------------------------------------------------------
# Assumes this has already been through 06_gauging_table_prep.R -- adjust
# filename if you're working from ssn703_gaugings_raw.csv instead (in which
# case rating_curve_period / stage_source / Stage_avg_corrected won't exist
# yet and this script should run before, not after, 06).

gaugings <- read_csv(
  file.path(meta_dir, "ssn703_gaugings_prepped.csv"),
  show_col_types = FALSE
)

message("Loaded ", nrow(gaugings), " gaugings from current table")


# -----------------------------------------------------------------------------
# 2. Load her original table -- force Date/Start_time to text so read_csv
#    doesn't silently type-guess Start_time as an hms column with seconds
#    appended, which breaks ymd_hm() parsing downstream (this bit us once
#    already -- see conversation history / script changelog)
# -----------------------------------------------------------------------------

old_raw <- read_csv(
  file.path(meta_dir, "MK_original_gauging_table_703.csv"),
  col_types = cols(Date = col_character(), Start_time = col_character(), .default = col_guess()),
  show_col_types = FALSE
)

message("Loaded ", nrow(old_raw), " rows from historical gauging table")

old_gaugings <- old_raw |>
  mutate(
    # Her Date + Start_time are LOCAL PST
    datetime_pst = ymd_hm(paste(Date, Start_time), tz = "Etc/GMT+8")
  )

n_parse_failed <- sum(is.na(old_gaugings$datetime_pst))
if (n_parse_failed > 0) {
  warning(n_parse_failed, " rows in historical table failed datetime parsing -- check Date/Start_time format")
}

old_gaugings <- old_gaugings |>
  filter(!is.na(datetime_pst),
         datetime_pst >= RC1_TAIL_START,
         datetime_pst <= RC1_TAIL_END) |>
  select(datetime_pst, Stage_avg_historical = Stage_avg, Q_meas_old = Q_meas)

message("Historical table rows in transition window (2018-09-14 to 2019-02-09): ",
        nrow(old_gaugings))

if (nrow(old_gaugings) == 0) {
  stop("No historical rows found in the transition window -- check date parsing / filter bounds before proceeding")
}


# -----------------------------------------------------------------------------
# 3. Identify current-table events in the same transition window
# -----------------------------------------------------------------------------
# Current table's datetime is UTC -- convert to PST before matching against
# her local-time records

transition_events <- gaugings |>
  filter(datetime >= RC1_TAIL_START, datetime <= RC1_TAIL_END) |>
  mutate(datetime_pst = with_tz(force_tz(datetime, tzone = "UTC"), tzone = "Etc/GMT+8"))

message("Current table events in transition window: ", nrow(transition_events))


# -----------------------------------------------------------------------------
# 4. Nearest-timestamp match (tolerance: 10 minutes)
# -----------------------------------------------------------------------------

matched <- transition_events |>
  rowwise() |>
  mutate(
    match_idx     = which.min(abs(old_gaugings$datetime_pst - datetime_pst)),
    time_diff_min = as.numeric(abs(old_gaugings$datetime_pst[match_idx] - datetime_pst), units = "mins"),
    Stage_avg_historical = if_else(time_diff_min <= 10, old_gaugings$Stage_avg_historical[match_idx], NA_real_),
    Q_meas_old            = if_else(time_diff_min <= 10, old_gaugings$Q_meas_old[match_idx], NA_real_)
  ) |>
  ungroup()

message("\nMatched: ", sum(!is.na(matched$Stage_avg_historical)),
        " of ", nrow(matched), " transition-window events")


# -----------------------------------------------------------------------------
# 5. Checks before applying anything
# -----------------------------------------------------------------------------

# 5a. Unmatched events -- expect these to be Final_rating_curve == "N"
#     (i.e. she excluded them from her fit too, so no match is expected)
message("\nUnmatched events (expect Final_rating_curve == 'N'):")
matched |>
  filter(is.na(Stage_avg_historical)) |>
  select(EventID, MID, datetime_pst, Final_rating_curve) |>
  print()

# 5b. Q sanity check -- discharge is sensor-independent, so Q_meas should
#     match closely for every matched row. Large discrepancies here mean
#     the match is wrong (bad timestamp pairing) and should NOT be applied
#     without investigating further.
message("\nQ_meas discrepancies (flag anything > 0.05 m3/s for review):")
q_check <- matched |>
  filter(!is.na(Q_meas_old)) |>
  mutate(q_diff = abs(Q_meas - Q_meas_old)) |>
  select(EventID, MID, datetime_pst, Q_meas, Q_meas_old, q_diff) |>
  arrange(desc(q_diff))

print(q_check)

n_q_mismatch <- sum(q_check$q_diff > 0.05, na.rm = TRUE)
if (n_q_mismatch > 0) {
  warning(n_q_mismatch, " matched rows have Q_meas mismatch > 0.05 m3/s -- ",
          "review before trusting the stage match for these rows; a Q mismatch ",
          "suggests the nearest-timestamp match paired the wrong events")
}


# -----------------------------------------------------------------------------
# 6. Apply the correction
# -----------------------------------------------------------------------------
# Only applies where a match was found within tolerance AND Q_meas agrees
# (protects against a bad nearest-timestamp match silently overwriting a
# correct current-table value with an unrelated historical one)

gaugings_corrected <- gaugings |>
  left_join(
    matched |>
      mutate(q_diff = abs(Q_meas - Q_meas_old)) |>
      mutate(safe_to_apply = !is.na(Stage_avg_historical) & (is.na(q_diff) | q_diff <= 0.05)) |>
      select(EventID, MID, Stage_avg_historical, safe_to_apply),
    by = c("EventID", "MID")
  ) |>
  mutate(
    Stage_avg_corrected = if_else(coalesce(safe_to_apply, FALSE),
                                   Stage_avg_historical,
                                   Stage_avg_corrected),
    stage_source = if_else(coalesce(safe_to_apply, FALSE),
                            "historical_ssn703b_recovered",
                            stage_source),
    rating_curve_period = if_else(coalesce(safe_to_apply, FALSE),
                                   "RC1",
                                   rating_curve_period),
    stage_status = if_else(coalesce(safe_to_apply, FALSE),
                            "ok",
                            stage_status)
  ) |>
  select(-Stage_avg_historical, -safe_to_apply)

message("\nApplied historical correction to ",
        sum(gaugings_corrected$stage_source == "historical_ssn703b_recovered", na.rm = TRUE),
        " rows")


# -----------------------------------------------------------------------------
# 7. Save
# -----------------------------------------------------------------------------

write_csv(gaugings_corrected, file.path(meta_dir, "ssn703_gaugings_prepped_corrected.csv"))

message("\nSaved: ", file.path(meta_dir, "ssn703_gaugings_prepped_corrected.csv"))
message("\nSummary of stage_source after correction:")
gaugings_corrected |> count(stage_source) |> print()

message("\nSummary of rating_curve_period after correction:")
gaugings_corrected |> count(rating_curve_period) |> print()

message("\nNext step: review the corrected table, especially the",
        "\n'historical_ssn703b_recovered' rows, before using for curve fitting.")




gaugings_prepped_corrected <- read_csv("03_docs/metadata/ssn703_gaugings_prepped_corrected.csv")
gaugings_prepped_corrected |>
  filter(stage_source == "historical_ssn703b_recovered") |>
  select(EventID, MID, datetime, Stage_avg_corrected, rating_curve_period)


gaugings_corrected <- gaugings_corrected |>
  mutate(Old_New = if_else(rating_curve_period == "RC1", "Old", "New"))

# Does the file that should have been backed up actually exist at that path?
file.exists("03_docs/metadata/ssn703_gaugings_prepped.csv")
file.copy(
  "03_docs/metadata/ssn703_gaugings_prepped.csv",
  paste0("03_docs/metadata/ssn703_gaugings_prepped_preRC1fix_", Sys.Date(), ".csv")
)

file.copy(
  "03_docs/metadata/ssn703_gaugings_prepped_corrected.csv",
  "03_docs/metadata/ssn703_gaugings_prepped.csv",
  overwrite = TRUE
)
