library(readr)
library(dplyr)
library(lubridate)
library(zoo)

# ---- Load the RC5-trimmed stage file from step 1 ----
# NOTE: write_csv() always serializes POSIXct as UTC ISO8601 (T...Z),
# regardless of the tz it was tagged with when written. So we let readr
# auto-parse it (as UTC) and then explicitly convert back to PST.
ssn703_d_rc5 <- read_csv(
  "C:/Users/Emily/Documents/git-repos/hydro_qaqc/01_raw/SSN703/ssn703_d_rc5_trimmed.csv",
  col_types = cols(
    measurement_time = col_datetime(),  # auto-parses ISO8601 T...Z as UTC
    year = col_integer(),
    month = col_character(),
    water_year = col_character(),
    stage_raw = col_double(),
    stage_avg = col_double(),
    stage_min = col_double(),
    stage_max = col_double(),
    stage_std = col_double()
  )
) %>%
  mutate(measurement_time = with_tz(measurement_time, "Etc/GMT+8"))

# =====================================================================
# TUNABLE PARAMETERS - adjust these for SSN703 PT4
# =====================================================================
range_min        <- 0.3  # lowest physically plausible stage (m)
range_max        <- 3    # highest physically plausible stage (m)
spike_threshold  <- 0.05 # m per 5-min step; flags |delta| exceeding this
flatline_run_n   <- 12   # consecutive identical values to flag (12 = 1 hr @ 5min)
gap_min_run      <- 1    # consecutive NA timesteps to flag (1 = flag every gap)
baseline_window  <- 2017 # ~7 days @ 5min, must be odd (runmed requirement)
shift_threshold  <- 0.15 # m; deviation from rolling baseline considered a sustained shift
shift_persist_n  <- 6    # min consecutive points before flagging (30 min) - avoids single-point noise
# =====================================================================

n <- nrow(ssn703_d_rc5)
stage <- ssn703_d_rc5$stage_avg

# ---- Range check ----
flag_range <- rep(FALSE, n)
if (!is.na(range_min)) flag_range <- flag_range | (stage < range_min)
if (!is.na(range_max)) flag_range <- flag_range | (stage > range_max)
flag_range[is.na(stage)] <- FALSE  # NAs handled by gap check, not range

# ---- Spike check (rate of change) ----
delta <- c(NA, diff(stage))
flag_spike <- !is.na(delta) & abs(delta) > spike_threshold

# ---- Flatline check (run-length of identical consecutive values) ----
# Uses rle() on stage; NAs break runs automatically
r <- rle(stage)
run_flag_vec <- rep(r$lengths >= flatline_run_n & !is.na(r$values), r$lengths)
flag_flatline <- run_flag_vec

# ---- Gap check (run-length of NA) ----
is_na_vec <- is.na(stage)
r_na <- rle(is_na_vec)
gap_flag_vec <- rep(r_na$values & r_na$lengths >= gap_min_run, r_na$lengths)
flag_gap <- gap_flag_vec

# ---- Sustained baseline-shift check ----
# Catches ramps/plateaus that ramp up over many small steps (each under the
# spike threshold) and hold at an unusual level for an extended period -
# invisible to point-to-point spike detection. Compares each value to a
# rolling median (baseline_window wide) and flags sustained deviations.
# NAs are temporarily interpolated ONLY for computing the baseline smooth -
# actual stage/NA values elsewhere are untouched.
stage_filled <- na.approx(stage, na.rm = FALSE)
stage_filled <- na.locf(stage_filled, na.rm = FALSE)
stage_filled <- na.locf(stage_filled, fromLast = TRUE, na.rm = FALSE)

baseline <- runmed(stage_filled, k = baseline_window, endrule = "median")
residual <- stage - baseline

is_shifted <- !is.na(residual) & abs(residual) > shift_threshold
r_shift <- rle(is_shifted)
flag_shift <- rep(r_shift$values & r_shift$lengths >= shift_persist_n, r_shift$lengths)

# ---- Combine into single flag_auto column ----
# Priority order when multiple checks trigger: GAP > RANGE > SHIFT > SPIKE > FLATLINE
ssn703_d_rc5 <- ssn703_d_rc5 %>%
  mutate(
    flag_range    = flag_range,
    flag_spike    = flag_spike,
    flag_flatline = flag_flatline,
    flag_gap      = flag_gap,
    flag_shift    = flag_shift,
    baseline      = baseline,
    flag_auto = case_when(
      flag_gap      ~ "GAP",
      flag_range    ~ "RANGE",
      flag_shift    ~ "SHIFT",
      flag_spike    ~ "SPIKE",
      flag_flatline ~ "FLATLINE",
      TRUE          ~ NA_character_
    )
  )

# ---- Summary ----
cat("Total rows:", n, "\n")
cat("Range flags:   ", sum(flag_range, na.rm = TRUE), "\n")
cat("Spike flags:   ", sum(flag_spike, na.rm = TRUE), "\n")
cat("Flatline flags:", sum(flag_flatline, na.rm = TRUE), "\n")
cat("Gap flags:     ", sum(flag_gap, na.rm = TRUE), "\n")
cat("Shift flags:   ", sum(flag_shift, na.rm = TRUE), "\n")
cat("Any flag_auto: ", sum(!is.na(ssn703_d_rc5$flag_auto)), "\n")

# ---- Save for next step (interactive review) ----
write_csv(ssn703_d_rc5,
          "C:/Users/Emily/Documents/git-repos/hydro_qaqc/01_raw/SSN703/ssn703_d_rc5_autoflagged.csv")
