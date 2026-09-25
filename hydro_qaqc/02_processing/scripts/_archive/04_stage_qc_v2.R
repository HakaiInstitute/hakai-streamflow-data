# =============================================================================
# SSN703 Stage QC - Script 04 (v2): Spike Detection, Flagging, and Gap Filling
# =============================================================================
# Purpose:
#   Apply quality control to the corrected SSN703 stage record. This includes:
#   1. Spike detection and flagging
#   2. Bad data period flagging from sensor metadata
#   3. Gap filling using a tiered approach (RESTRUCTURED in this version):
#      Tier 1 -- spline interpolation for gaps < SHORT_GAP_MAX_MINS (5 hr)
#      Tier 2 -- SA sensor relationship for gaps >= SHORT_GAP_MAX_MINS,
#                applied row-by-row wherever an SA reading exists
#      Tier 3 -- SSN844 (separate watershed) relationship for gaps >=
#                SHORT_GAP_MAX_MINS where SA has no reading, applied
#                row-by-row wherever an SSN844 reading exists
#      Tier 4 -- Leave as NA where none of the above have coverage
#
# CHANGE LOG (v2, this version):
#   - RESTRUCTURED tier order and priority per updated requirements:
#       * Previous version tried SA first regardless of gap length, then
#         spline for gaps <= 180 min. This version instead uses gap DURATION
#         as the primary branch: short gaps (<= 5 hr) get spline
#         interpolation (unchanged method, na.spline); long gaps (> 5 hr)
#         get the SA/SSN844 relationship tiers instead of being attempted via
#         SA regardless of length. The threshold itself also changed from
#         180 min to 300 min (5 hr).
#       * SA relationship is now applied WITHOUT an R2 gate -- if a
#         regression can be fit (minimum overlapping sample size met), it is
#         used regardless of fit quality. This is a deliberate loosening from
#         v1's R2 >= 0.95 threshold; fit quality is no longer checked here.
#       * Added a new Tier 3: SSN844 cross-watershed relationship, used only
#         where SA has no reading. NOTE: SSN844 and SSN703 are different
#         watersheds with different catchment sizes/response times, so this
#         is a materially rougher proxy than the same-site SA relationship --
#         treat SSN844-filled values as lower-confidence.
#   - CARRIED FORWARD from v1 bug fixes:
#       * gap_id now increments on gap STARTS (is_gap & !lag(is_gap)), not on
#         valid->valid transitions, so closely-spaced separate gaps don't get
#         merged into one inflated block.
#       * A validation pass after all fill tiers checks whether every row
#         flagged as filled actually received a non-NA value, and downgrades
#         to unfilled/bad_data if not (this can still happen at the very
#         edges of the deployment window, or if neither SA nor SSN844 had a
#         reading at a given long-gap timestamp).
#   - NEW FIX (this version): spike detection was previously blind to spikes
#     sitting immediately adjacent to bad_data block boundaries, because it
#     compared each row only to lag(stage_qc) and a rollmedian() window
#     computed directly on stage_qc -- both NA whenever the neighboring
#     row/window falls inside a bad_data period, which silently suppressed
#     spike flagging exactly where it was most needed (found via gap audit
#     on ssn703_c, Jul-Aug 2023, showing erratic unflagged single-point jumps
#     next to bad_data boundaries). Fixed by comparing against the nearest
#     PRIOR VALID reading via na.locf lookback (time-normalized rate, not
#     assuming fixed 5-min spacing), and switching the rolling median check
#     to a na.rm = TRUE / partial-window version so it still returns a value
#     near gap edges. See the FIX comment in Section 3 for full detail.
#
# Flags applied:
#   "raw"                -- no QC applied, original value
#   "spike"              -- detected as spike, value set to NA, eligible for filling
#   "bad_data"           -- confirmed bad period from metadata; unfillable after all tiers
#   "gf_spline"              -- transmission gap < 5hr; filled via spline interpolation (baseflow)
#   "gf_spline_event"        -- as above, during event conditions (use with caution)
#   "replaced_spline"        -- was bad_data/spike; replaced via spline interpolation (baseflow)
#   "replaced_spline_event"  -- as above, during event conditions
#   "gf_sa"              -- transmission gap >= 5hr; filled using SA relationship
#   "replaced_sa"        -- was bad_data/spike; replaced using SA relationship
#   "gf_ssn844"          -- transmission gap >= 5hr, no SA coverage; filled using SSN844 relationship
#   "replaced_ssn844"    -- was bad_data/spike, no SA coverage; replaced using SSN844 relationship
#   "unfilled"           -- no coverage from any tier; remains NA
#
# Inputs:
#   02_processing/data_parsed/ssn703_corrected.rds
#   SSN844 stage data (path set below -- confirm actual file/column names)
#
# Outputs:
#   02_processing/data_parsed/ssn703_qc.rds
#   02_processing/plots/ssn703_qc_summary.pdf
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)
library(zoo)        # for rollapply, na.locf, and na.spline


# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

plot_dir <- "02_processing/plots"
data_dir <- "02_processing/data_parsed"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)

# QC parameters -- adjust here, not inline
SPIKE_RATE_M_PER_5MIN <- 0.03   # max plausible stage change in 5 minutes (m)
SPIKE_WINDOW           <- 5     # rolling median window (number of timesteps)
SHORT_GAP_MAX_MINS   <- 300   # gaps shorter than this (5 hr) use spline interpolation
MIN_OVERLAP_N          <- 100   # minimum overlapping clean data points to fit a
# relationship (SA or SSN844) -- no R2 gate applied
EVENT_STAGE_THRESHOLD  <- 0.60  # stage above which conditions are considered event-like

# NOTE: confirm this path and the stage column name against the actual
# SSN844 output before running -- this assumes a similar structure to the
# primary/SA data (a `timestamp` column and a stage column).
ssn844_stage_path <- "02_processing/data_parsed/ssn844_qc.rds"
ssn844_stage_col  <- "stage_qc"   # NOTE: confirm this is the right column name

sensor_colours <- c(
  "ssn703_a"  = "#E41A1C",
  "ssn703_b"  = "#377EB8",
  "ssn703_c"  = "#4DAF4A",
  "ssn703_d"  = "#984EA3",
  "ssn703_sa" = "#FF7F00"
)


# -----------------------------------------------------------------------------
# 1. Load data
# -----------------------------------------------------------------------------

stage <- readRDS(file.path(data_dir, "ssn703_corrected.rds"))

# Separate primary sensors and SA sensor
primary <- stage |>
  filter(sensor_role == "primary") |>
  arrange(site_id, timestamp)

sa <- stage |>
  filter(site_id == "ssn703_sa") |>
  select(timestamp, stage_sa = stage_corrected) |>
  arrange(timestamp)

# SSN844 cross-watershed relationship data
# NOTE: adjust column selection below once the actual SSN844 file/column
# names are confirmed.
ssn844 <- readRDS(ssn844_stage_path) |>
  select(timestamp, stage_ssn844 = all_of(ssn844_stage_col)) |>
  arrange(timestamp)

message("Loaded ", nrow(primary), " rows across ",
        n_distinct(primary$site_id), " primary sensors")
message("Loaded ", nrow(sa), " rows of SA sensor data")
message("Loaded ", nrow(ssn844), " rows of SSN844 data")


# -----------------------------------------------------------------------------
# 1b. Trim each sensor to its valid deployment window
# -----------------------------------------------------------------------------
# date_start and date_end from sensor_registry define when each sensor was
# actually deployed. Data outside this window is not valid and must be removed
# before any QC is applied -- otherwise interpolation and spike detection
# operate on out-of-range data and produce nonsense results.
#
# date_end = NA means the sensor is ongoing -- no upper trim applied.

n_before_trim <- nrow(primary)

primary <- primary |>
  filter(
    timestamp >= date_start,
    is.na(date_end) | timestamp <= date_end
  )

n_trimmed <- n_before_trim - nrow(primary)
message("\nRows trimmed outside deployment window: ", n_trimmed)
message("Rows remaining: ", nrow(primary))

# Confirm date ranges after trimming -- verify against sensor_registry
primary |>
  group_by(site_id) |>
  summarise(
    start = min(timestamp, na.rm = TRUE),
    end   = max(timestamp, na.rm = TRUE),
    n     = n(),
    .groups = "drop"
  ) |>
  print()


# -----------------------------------------------------------------------------
# 2. Flag bad data periods from metadata
# -----------------------------------------------------------------------------
# Periods where bad_data_start is set in sensor_registry are flagged before
# spike detection -- these are known bad periods, not algorithmically detected.

primary <- primary |>
  mutate(
    qc_flag = case_when(
      !is.na(bad_data_start) & timestamp >= bad_data_start &
        (is.na(bad_data_end) | timestamp <= bad_data_end) ~ "bad_data",
      TRUE ~ "raw"
    ),
    stage_qc = if_else(qc_flag == "bad_data", NA_real_, stage_corrected)
  )

n_bad <- sum(primary$qc_flag == "bad_data")
message("\nBad data periods flagged: ", n_bad, " rows")
message("Note: bad_data rows will be replaced via spline interpolation, SA,")
message("      or SSN844 relationship where possible. Only rows unfillable")
message("      after all tiers will retain the bad_data flag")


# -----------------------------------------------------------------------------
# 3. Spike detection
# -----------------------------------------------------------------------------
# Spikes are detected using rate of change: if stage changes more than
# SPIKE_RATE_M_PER_5MIN (normalized per 5 min of elapsed time) relative to
# the last known-good reading, the value is flagged as a spike.
#
# A rolling median crosscheck is also applied: if a value deviates from the
# rolling median by more than 3x the spike threshold, it is flagged.
# This catches isolated single-point spikes that may not show large
# rate-of-change if surrounded by other bad values.
#
# Only applied to rows not already flagged as bad_data.
#
# FIX (found via gap audit on ssn703_c, Jul-Aug 2023): the original version
# compared each row only to lag(stage_qc) and a rollmedian() window computed
# directly on stage_qc. Both of those are NA wherever the immediately
# preceding row (or window) falls inside a bad_data period -- so a row
# sitting right at the edge of a bad_data block had its rate-of-change and
# rolling-median checks silently return NA, and NA & FALSE evaluates to
# FALSE in the old `is_spike & !is.na(is_spike)` guard -- meaning genuine
# spikes immediately adjacent to bad_data runs were never flagged at all.
# This is exactly the pattern seen in the audit: erratic single-point jumps
# sitting right next to bad_data block boundaries, never caught.
#
# Fix: instead of the immediately-previous row, compare against the nearest
# PRIOR VALID reading regardless of how many NA rows sit in between (via
# na.locf lookback), and normalize the resulting rate by actual elapsed time
# rather than assuming fixed 5-min spacing (since the gap in between may not
# be exactly one timestep). The rolling median check is switched from
# rollmedian() (which requires a complete window with no NAs) to a
# na.rm = TRUE, partial-window version, so it still produces a usable value
# near gap edges instead of silently going NA.

primary <- primary |>
  group_by(site_id) |>
  arrange(timestamp) |>
  mutate(
    # Last known-good value/time, carried forward across any intervening NAs
    # (bad_data, spike, or transmission gap rows) -- not just the
    # immediately-previous row.
    last_valid_stage = zoo::na.locf(stage_qc, na.rm = FALSE),
    last_valid_time   = if_else(!is.na(stage_qc), timestamp, as.POSIXct(NA)),
    last_valid_time   = zoo::na.locf(last_valid_time, na.rm = FALSE),
    elapsed_mins      = as.numeric(difftime(timestamp, last_valid_time, units = "mins")),
    # Rate of change, normalized to a per-5-min basis so it's comparable to
    # SPIKE_RATE_M_PER_5MIN even when the last valid reading was more than
    # one timestep ago.
    stage_rate = if_else(
      !is.na(elapsed_mins) & elapsed_mins > 0,
      abs(stage_qc - last_valid_stage) / elapsed_mins * 5,
      NA_real_
    ),
    # Rolling median with na.rm = TRUE and partial windows, so it still
    # returns a value near bad_data boundaries instead of requiring a
    # completely NA-free window like rollmedian() does.
    rolling_med = rollapply(
      stage_qc, width = SPIKE_WINDOW,
      FUN = function(x) if (all(is.na(x))) NA_real_ else median(x, na.rm = TRUE),
      fill = NA, align = "center", partial = TRUE
    ),
    rolling_dev = abs(stage_qc - rolling_med),
    is_spike    = qc_flag == "raw" &
      ((!is.na(stage_rate) & stage_rate > SPIKE_RATE_M_PER_5MIN) |
         (!is.na(rolling_dev) & rolling_dev > 3 * SPIKE_RATE_M_PER_5MIN)),
    qc_flag     = if_else(is_spike & !is.na(is_spike), "spike", qc_flag),
    stage_qc    = if_else(qc_flag == "spike", NA_real_, stage_qc)
  ) |>
  select(-last_valid_stage, -last_valid_time, -elapsed_mins, -stage_rate,
         -rolling_med, -rolling_dev, -is_spike) |>
  ungroup()

n_spikes <- sum(primary$qc_flag == "spike")
message("Spikes flagged: ", n_spikes, " rows")
message("(includes spikes adjacent to bad_data boundaries, which were previously missed --")
message(" see FIX comment above this section for details)")


# -----------------------------------------------------------------------------
# 4. Fit SA and SSN844 relationships (once per sensor, applied later row-wise)
# -----------------------------------------------------------------------------
# Relationships are fit on clean raw data only (qc_flag == "raw"). No R2 gate
# is applied here -- if the minimum overlap sample size is met, the
# relationship is used regardless of fit quality. Applied later on a
# row-by-row basis within long (>= SHORT_GAP_MAX_MINS) gaps: SA is tried
# first, SSN844 only where SA itself has no reading at that timestamp.

fit_relationship <- function(df, other_df, other_col, label, min_n) {
  
  site <- unique(df$site_id)
  
  df_joined <- df |>
    left_join(other_df, by = "timestamp")
  
  fit_data <- df_joined |>
    filter(qc_flag == "raw", !is.na(stage_qc), !is.na(.data[[other_col]]))
  
  if (nrow(fit_data) < min_n) {
    message("  ", site, ": insufficient clean overlapping ", label,
            " data (", nrow(fit_data), " < ", min_n, ") -- skipping")
    return(list(fit = NULL, joined = df_joined))
  }
  
  form <- as.formula(paste0("stage_qc ~ ", other_col))
  fit  <- lm(form, data = fit_data)
  r2   <- summary(fit)$r.squared
  
  message("  ", site, ": ", label, " relationship fit on ", nrow(fit_data),
          " points, R2 = ", round(r2, 4), " (no R2 gate applied -- used regardless of fit quality)")
  
  list(fit = fit, joined = df_joined)
}

# Apply per sensor, storing predictions for use in Section 5
primary <- primary |>
  group_by(site_id) |>
  group_modify(~ {
    
    site <- unique(.x$site_id)
    message("\n  Fitting relationships for ", site)
    
    sa_result <- fit_relationship(.x, sa, "stage_sa", "SA", MIN_OVERLAP_N)
    df <- sa_result$joined
    df$stage_sa_predicted <- if (!is.null(sa_result$fit)) {
      predict(sa_result$fit, newdata = data.frame(stage_sa = df$stage_sa))
    } else {
      NA_real_
    }
    
    ssn844_result <- fit_relationship(.x, ssn844, "stage_ssn844", "SSN844", MIN_OVERLAP_N)
    df <- ssn844_result$joined |>
      left_join(df |> select(timestamp, stage_sa_predicted), by = "timestamp")
    df$stage_ssn844_predicted <- if (!is.null(ssn844_result$fit)) {
      predict(ssn844_result$fit, newdata = data.frame(stage_ssn844 = df$stage_ssn844))
    } else {
      NA_real_
    }
    
    df |> select(-stage_sa, -stage_ssn844)
  }) |>
  ungroup()


# -----------------------------------------------------------------------------
# 5. Gap filling -- duration-based tiering
# -----------------------------------------------------------------------------
# Flag logic:
#   - Gaps < SHORT_GAP_MAX_MINS: spline interpolation
#     (transmission origin -> gf_spline / gf_spline_event;
#      bad_data/spike origin -> replaced_spline / replaced_spline_event)
#   - Gaps >= SHORT_GAP_MAX_MINS: SA relationship first (row-wise, wherever
#     an SA reading exists at that timestamp), then SSN844 relationship
#     wherever SA has no reading
#     (transmission origin -> gf_sa / gf_ssn844;
#      bad_data/spike origin -> replaced_sa / replaced_ssn844)
#   - No coverage from any tier -- retain original flag (bad_data stays
#     bad_data, transmission gap -> unfilled)

fill_gaps <- function(df, short_gap_max_mins, event_threshold) {
  
  site <- unique(df$site_id)
  
  df <- df |>
    arrange(timestamp) |>
    mutate(
      original_flag = qc_flag,
      stage_for_gaps = if_else(qc_flag %in% c("bad_data", "spike"),
                               NA_real_, stage_qc),
      is_gap  = is.na(stage_for_gaps),
      # gap_id increments on gap STARTS, not valid->valid transitions --
      # this is the v1 fix, carried forward here. Prevents closely-spaced
      # separate gaps from being merged into one inflated block.
      gap_id  = cumsum(is_gap & !lag(is_gap, default = FALSE))
    )
  
  gap_info <- df |>
    filter(is_gap) |>
    group_by(gap_id) |>
    summarise(
      gap_start    = min(timestamp),
      gap_end      = max(timestamp),
      gap_mins     = as.numeric(difftime(max(timestamp), min(timestamp), units = "mins")),
      any_bad_data = any(original_flag == "bad_data"),
      .groups = "drop"
    )
  
  if (nrow(gap_info) == 0) {
    message("  ", site, ": no gaps to fill")
    return(df |> select(-is_gap, -gap_id, -original_flag, -stage_for_gaps))
  }
  
  gap_info <- gap_info |>
    rowwise() |>
    mutate(
      stage_before = df |>
        filter(timestamp < gap_start, !is.na(stage_for_gaps)) |>
        slice_tail(n = 1) |>
        pull(stage_for_gaps) |>
        (\(x) if (length(x) == 0) NA_real_ else x)(),
      stage_after = df |>
        filter(timestamp > gap_end, !is.na(stage_for_gaps)) |>
        slice_head(n = 1) |>
        pull(stage_for_gaps) |>
        (\(x) if (length(x) == 0) NA_real_ else x)(),
      is_event_gap = (!is.na(stage_before) & stage_before > event_threshold) |
        (!is.na(stage_after)  & stage_after  > event_threshold),
      is_short     = gap_mins < short_gap_max_mins
    ) |>
    ungroup()
  
  n_short <- sum(gap_info$is_short)
  n_long  <- sum(!gap_info$is_short)
  message("  ", site, ": ", n_short, " short gaps (< ", short_gap_max_mins,
          " min) -> spline interpolation tier")
  message("  ", site, ": ", n_long, " long gaps (>= ", short_gap_max_mins,
          " min) -> SA / SSN844 relationship tier")
  
  short_gap_ids <- gap_info |> filter(is_short) |> pull(gap_id)
  long_gap_ids  <- gap_info |> filter(!is_short) |> pull(gap_id)
  
  # spline interpolation, restricted to short-eligible gap_ids only
  df <- df |>
    mutate(
      short_eligible   = is_gap & (gap_id %in% short_gap_ids),
      stage_for_spline = if_else(short_eligible | !is_gap, stage_for_gaps, NA_real_),
      stage_splined    = na.spline(stage_for_spline, x = timestamp, na.rm = FALSE)
    )
  
  df <- df |>
    left_join(
      gap_info |> select(gap_id, gap_mins, is_event_gap, any_bad_data, is_short),
      by = "gap_id"
    ) |>
    mutate(
      long_eligible = is_gap & (gap_id %in% long_gap_ids),
      qc_flag = case_when(
        # --- Long gaps: SA first, SSN844 fallback (row-wise on data availability) ---
        long_eligible & !is.na(stage_sa_predicted) & any_bad_data                     ~ "replaced_sa",
        long_eligible & !is.na(stage_sa_predicted)                                    ~ "gf_sa",
        long_eligible & is.na(stage_sa_predicted) & !is.na(stage_ssn844_predicted) & any_bad_data ~ "replaced_ssn844",
        long_eligible & is.na(stage_sa_predicted) & !is.na(stage_ssn844_predicted)    ~ "gf_ssn844",
        # --- Short gaps: spline interpolation ---
        short_eligible & !is.na(stage_splined) & is_event_gap  & any_bad_data ~ "replaced_spline_event",
        short_eligible & !is.na(stage_splined) & !is_event_gap & any_bad_data ~ "replaced_spline",
        short_eligible & !is.na(stage_splined) & is_event_gap                 ~ "gf_spline_event",
        short_eligible & !is.na(stage_splined) & !is_event_gap                ~ "gf_spline",
        # --- No coverage from any tier ---
        is_gap & any_bad_data ~ "bad_data",
        is_gap                ~ "unfilled",
        TRUE ~ qc_flag
      ),
      stage_qc = case_when(
        qc_flag %in% c("gf_sa", "replaced_sa")             ~ stage_sa_predicted,
        qc_flag %in% c("gf_ssn844", "replaced_ssn844")     ~ stage_ssn844_predicted,
        qc_flag %in% c("gf_spline", "gf_spline_event",
                       "replaced_spline", "replaced_spline_event") ~ stage_splined,
        TRUE ~ stage_qc
      )
    )
  
  # -------------------------------------------------------------------------
  # Validation: confirm every row flagged as filled actually got a value.
  # Can still fail at deployment-window edges (spline tier) or where neither
  # SA nor SSN844 had a reading at a specific long-gap timestamp -- downgrade
  # those rather than shipping a "filled" flag on an empty value.
  # -------------------------------------------------------------------------
  fill_flags <- c("gf_spline", "gf_spline_event", "replaced_spline", "replaced_spline_event",
                  "gf_sa", "replaced_sa", "gf_ssn844", "replaced_ssn844")
  
  n_before_validation <- sum(df$qc_flag %in% fill_flags & is.na(df$stage_qc))
  
  df <- df |>
    mutate(
      qc_flag = if_else(
        qc_flag %in% fill_flags & is.na(stage_qc),
        if_else(any_bad_data, "bad_data", "unfilled"),
        qc_flag
      )
    )
  
  if (n_before_validation > 0) {
    message("  ", site, ": WARNING -- ", n_before_validation,
            " rows were flagged as filled but no tier actually produced a value. ",
            "Downgraded to unfilled/bad_data.")
  }
  
  df |>
    select(-is_gap, -gap_id, -gap_mins, -is_event_gap, -any_bad_data, -is_short,
           -short_eligible, -long_eligible, -stage_for_spline, -stage_splined,
           -stage_sa_predicted, -stage_ssn844_predicted,
           -original_flag, -stage_for_gaps)
}

primary <- primary |>
  group_by(site_id) |>
  group_modify(~ fill_gaps(.x, SHORT_GAP_MAX_MINS, EVENT_STAGE_THRESHOLD)) |>
  ungroup()


# -----------------------------------------------------------------------------
# 6. QC summary
# -----------------------------------------------------------------------------

message("\n--- QC flag summary ---")
primary |>
  count(site_id, qc_flag) |>
  pivot_wider(names_from = qc_flag, values_from = n, values_fill = 0) |>
  print()

# Sanity check: confirm no rows remain that are flagged as filled but empty.
n_flagged_but_empty <- primary |>
  filter(
    qc_flag %in% c("gf_spline", "gf_spline_event", "replaced_spline", "replaced_spline_event",
                   "gf_sa", "replaced_sa", "gf_ssn844", "replaced_ssn844"),
    is.na(stage_qc)
  ) |>
  nrow()

if (n_flagged_but_empty > 0) {
  warning(n_flagged_but_empty,
          " rows are flagged as filled but stage_qc is still NA -- ",
          "investigate before proceeding.")
} else {
  message("\nSanity check passed: no rows flagged as filled with an empty stage_qc value")
}


# -----------------------------------------------------------------------------
# 7. Validation plots -- one file per sensor per water year
# -----------------------------------------------------------------------------
# Splits each sensor's record by water year (Oct 1 - Sep 30) and saves one
# file per sensor per water year into a per-site subdirectory.

flag_colours <- c(
  "raw"                = "grey60",
  "bad_data"           = "#E41A1C",
  "spike"              = "#FF7F00",
  "gf_spline"              = "#74C2E1",
  "gf_spline_event"        = "#C994C7",
  "replaced_spline"        = "#377EB8",
  "replaced_spline_event"  = "#984EA3",
  "gf_sa"              = "#A6D96A",
  "replaced_sa"        = "#4DAF4A",
  "gf_ssn844"          = "#FDBF6F",
  "replaced_ssn844"    = "#B15928",
  "unfilled"           = "black"
)

primary <- primary |>
  mutate(
    water_year = if_else(month(timestamp) >= 10, year(timestamp) + 1, year(timestamp))
  )

plot_qc_sensor_year <- function(df, site, wy) {
  
  df |>
    filter(site_id == site, water_year == wy) |>
    ggplot(aes(x = timestamp, y = stage_qc, colour = qc_flag)) +
    geom_point(size = 0.5, na.rm = TRUE) +
    scale_colour_manual(values = flag_colours, drop = FALSE) +
    scale_x_datetime(date_breaks = "1 month", date_labels = "%b") +
    labs(
      title   = paste0("SSN703 -- QC flags: ", site, " -- Water Year ", wy),
      x       = NULL,
      y       = "Stage QC (m)",
      colour  = "QC flag",
      caption = paste0(
        "Spike threshold: ", SPIKE_RATE_M_PER_5MIN, " m per 5min | ",
        "Spline fill max: ", SHORT_GAP_MAX_MINS, " min | ",
        "Min overlap N: ", MIN_OVERLAP_N, " (no R2 gate)\n",
        "Plot generated by 04_stage_qc_v2.R"
      )
    ) +
    theme_bw() +
    theme(
      legend.position  = "bottom",
      panel.grid.minor = element_blank(),
      plot.caption     = element_text(hjust = 0, size = 8, colour = "grey40")
    )
}

qc_by_year_dir <- file.path(plot_dir, "qc_by_year")

for (site in unique(primary$site_id)) {
  
  site_dir <- file.path(qc_by_year_dir, site)
  dir.create(site_dir, showWarnings = FALSE, recursive = TRUE)
  
  site_years <- primary |>
    filter(site_id == site) |>
    pull(water_year) |>
    unique() |>
    sort()
  
  for (wy in site_years) {
    
    n_rows <- primary |>
      filter(site_id == site, water_year == wy) |>
      nrow()
    
    if (n_rows == 0) next
    
    p <- plot_qc_sensor_year(primary, site, wy)
    
    out_file <- file.path(site_dir, paste0(site, "_WY", wy, ".pdf"))
    ggsave(out_file, p, width = 14, height = 6)
    message("Saved: ", out_file)
  }
}

message("\nAll per-year plots saved under: ", qc_by_year_dir)


# -----------------------------------------------------------------------------
# 8. Save QC output
# -----------------------------------------------------------------------------

stage_qc <- bind_rows(
  primary,
  stage |> filter(site_id == "ssn703_sa")
)

saveRDS(stage_qc, file.path(data_dir, "ssn703_qc.rds"))

message("Saved: ", file.path(data_dir, "ssn703_qc.rds"))
message("\nNext steps:")
message("  1. Open the per-year plots under qc_by_year/ and check flags look reasonable")
message("  2. Confirm the SSN844 path/column names at the top of this script are correct --")
message("     this was a stated assumption, not verified against an actual file")
message("  3. Check how many rows landed in gf_ssn844/replaced_ssn844 -- these are the")
message("     lowest-confidence fills (cross-watershed proxy) and worth a closer look")
message("  4. If spike threshold is too aggressive or too lenient, adjust")
message("     SPIKE_RATE_M_PER_5MIN at the top of this script and rerun")
message("  5. Proceed to 05_stage_output.R")