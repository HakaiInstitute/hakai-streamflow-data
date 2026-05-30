# =============================================================================
# SSN703 Stage QC - Script 04: Spike Detection, Flagging, and Gap Filling
# =============================================================================
# Purpose:
#   Apply quality control to the corrected SSN703 stage record. This includes:
#   1. Spike detection and flagging
#   2. Bad data period flagging from sensor metadata
#   3. Gap filling using a tiered approach:
#      Tier 1 -- SA sensor relationship (where R2 >= 0.95 and SA data available)
#      Tier 2 -- Spline interpolation for gaps <= 3 hours during baseflow
#      Tier 3 -- Spline interpolation for gaps <= 3 hours during events (flagged)
#      Tier 4 -- Leave as NA for gaps > 3 hours without SA coverage
#
# Flags applied:
#   "raw"              -- no QC applied, original value
#   "spike"            -- detected as spike, value set to NA, eligible for filling
#   "bad_data"         -- confirmed bad period from metadata; unfillable after all tiers
#   "replaced_sa"      -- was bad_data or spike; replaced using SA sensor relationship
#   "replaced_spline"  -- was bad_data or spike; replaced using spline interpolation
#   "replaced_spline_event" -- as above but during event conditions (use with caution)
#   "gf_sa"            -- transmission gap; filled using SA sensor relationship
#   "gf_spline"        -- transmission gap; filled using spline interpolation (baseflow)
#   "gf_spline_event"  -- transmission gap; filled using spline interpolation (event)
#   "unfilled"         -- gap too long and no SA coverage; remains NA
#
# Inputs:
#   02_processing/data_parsed/ssn703_corrected.rds
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
library(zoo)        # for rollmedian and na.spline


# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

plot_dir <- "02_processing/plots"
data_dir <- "02_processing/data_parsed"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)

# QC parameters -- adjust here, not inline
SPIKE_RATE_M_PER_5MIN <- 0.03   # max plausible stage change in 5 minutes (m)
SPIKE_WINDOW           <- 5     # rolling median window (number of timesteps)
MAX_FILL_GAP_MINS      <- 180   # maximum gap to fill without SA (minutes)
SA_R2_THRESHOLD        <- 0.95  # minimum R2 to use SA relationship for gap filling
EVENT_STAGE_THRESHOLD  <- 0.60  # stage above which conditions are considered event-like
                                 # gap fills above this threshold get gf_spline_event flag

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

message("Loaded ", nrow(primary), " rows across ",
        n_distinct(primary$site_id), " primary sensors")
message("Loaded ", nrow(sa), " rows of SA sensor data")


# -----------------------------------------------------------------------------
# 1b. Trim each sensor to its valid deployment window
# -----------------------------------------------------------------------------
# date_start and date_end from sensor_registry define when each sensor was
# actually deployed. Data outside this window is not valid and must be removed
# before any QC is applied -- otherwise spline interpolation and spike detection
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
message("Note: bad_data rows will be replaced by SA or spline filling where possible")
message("      Only rows unfillable after all tiers will retain bad_data flag")


# -----------------------------------------------------------------------------
# 3. Spike detection
# -----------------------------------------------------------------------------
# Spikes are detected using rate of change: if stage changes more than
# SPIKE_RATE_M_PER_5MIN between consecutive timesteps in either direction,
# the value is flagged as a spike.
#
# A rolling median crosscheck is also applied: if a value deviates from the
# rolling median by more than 3x the spike threshold, it is flagged.
# This catches isolated single-point spikes that may not show large
# rate-of-change if surrounded by other bad values.
#
# Only applied to rows not already flagged as bad_data.

primary <- primary |>
  group_by(site_id) |>
  arrange(timestamp) |>
  mutate(
    # Rate of change between consecutive timesteps
    stage_diff    = abs(stage_qc - lag(stage_qc)),
    # Rolling median over SPIKE_WINDOW timesteps
    rolling_med   = rollmedian(stage_qc, k = SPIKE_WINDOW, fill = NA, align = "center"),
    rolling_dev   = abs(stage_qc - rolling_med),
    # Flag spikes
    is_spike      = qc_flag == "raw" &
                    (stage_diff > SPIKE_RATE_M_PER_5MIN |
                     rolling_dev > 3 * SPIKE_RATE_M_PER_5MIN),
    qc_flag       = if_else(is_spike & !is.na(is_spike), "spike", qc_flag),
    stage_qc      = if_else(qc_flag == "spike", NA_real_, stage_qc)
  ) |>
  select(-stage_diff, -rolling_med, -rolling_dev, -is_spike) |>
  ungroup()

n_spikes <- sum(primary$qc_flag == "spike")
message("Spikes flagged: ", n_spikes, " rows")


# -----------------------------------------------------------------------------
# 4. Gap filling -- Tier 1: SA sensor relationship
# -----------------------------------------------------------------------------
# For each primary sensor, assess the linear relationship between primary
# and SA stage over periods where both have clean data. If R2 >= SA_R2_THRESHOLD,
# use the SA relationship to fill:
#   - Transmission gaps (stage_qc is NA and qc_flag is not bad_data or spike)
#   - Bad data periods (qc_flag == "bad_data") --> replaced_sa
#   - Spike periods (qc_flag == "spike") --> replaced_sa
#
# Relationship is assessed on clean raw data only (qc_flag == "raw").

fill_with_sa <- function(df, sa_df, r2_threshold) {

  site <- unique(df$site_id)
  message("\n  Assessing SA relationship for ", site)

  # Join SA data to primary
  df_sa <- df |>
    left_join(sa_df, by = "timestamp")

  # Fit relationship on clean overlapping data only
  fit_data <- df_sa |>
    filter(qc_flag == "raw", !is.na(stage_qc), !is.na(stage_sa))

  if (nrow(fit_data) < 100) {
    message("  ", site, ": insufficient clean overlapping data for SA relationship -- skipping")
    df$gf_sa_used <- FALSE
    return(df)
  }

  fit <- lm(stage_qc ~ stage_sa, data = fit_data)
  r2  <- summary(fit)$r.squared

  message("  ", site, ": SA relationship R2 = ", round(r2, 4))

  if (r2 < r2_threshold) {
    message("  ", site, ": R2 below threshold (", r2_threshold, ") -- SA filling skipped")
    df$gf_sa_used <- FALSE
    return(df)
  }

  # Apply SA relationship to fill NAs and bad_data/spike rows where SA available
  df_sa <- df_sa |>
    mutate(
      stage_sa_predicted = predict(fit, newdata = data.frame(stage_sa = stage_sa)),
      # Determine fill flag based on original flag
      fill_flag = case_when(
        qc_flag %in% c("bad_data", "spike") & !is.na(stage_sa_predicted) ~ "replaced_sa",
        is.na(stage_qc) & !is.na(stage_sa_predicted)                      ~ "gf_sa",
        TRUE ~ NA_character_
      ),
      qc_flag  = if_else(!is.na(fill_flag), fill_flag, qc_flag),
      stage_qc = if_else(!is.na(fill_flag), stage_sa_predicted, stage_qc)
    ) |>
    select(-stage_sa, -stage_sa_predicted, -fill_flag)

  df_sa$gf_sa_used <- TRUE
  message("  ", site, ": SA gap filling applied")
  return(df_sa)
}

# Apply SA filling to each primary sensor separately
primary <- primary |>
  group_by(site_id) |>
  group_modify(~ fill_with_sa(.x, sa, SA_R2_THRESHOLD)) |>
  ungroup()


# -----------------------------------------------------------------------------
# 5. Gap filling -- Tiers 2 and 3: Spline interpolation
# -----------------------------------------------------------------------------
# For remaining NAs, bad_data, and spike rows after SA filling, use spline
# interpolation for gaps up to MAX_FILL_GAP_MINS.
#
# Flag logic:
#   - Transmission gaps filled --> gf_spline / gf_spline_event
#   - bad_data or spike rows filled --> replaced_spline / replaced_spline_event
#   - Gaps longer than MAX_FILL_GAP_MINS --> unfilled (NA)
#   - bad_data rows unfillable after all tiers --> retain bad_data flag

fill_with_spline <- function(df, max_gap_mins, event_threshold) {

  site <- unique(df$site_id)

  # Treat bad_data and spike rows as NA for gap identification
  df <- df |>
    arrange(timestamp) |>
    mutate(
      original_flag = qc_flag,
      stage_for_gaps = if_else(qc_flag %in% c("bad_data", "spike"),
                               NA_real_, stage_qc),
      is_gap  = is.na(stage_for_gaps),
      gap_id  = cumsum(!is_gap & lag(!is_gap, default = TRUE))
    )

  # For each gap, determine length and whether it's during an event
  gap_info <- df |>
    filter(is_gap) |>
    group_by(gap_id) |>
    summarise(
      gap_start  = min(timestamp),
      gap_end    = max(timestamp),
      gap_mins   = as.numeric(difftime(max(timestamp), min(timestamp),
                                       units = "mins")),
      # Track whether any rows in the gap were originally bad_data
      any_bad_data = any(original_flag == "bad_data"),
      .groups    = "drop"
    )

  if (nrow(gap_info) == 0) {
    message("  ", site, ": no gaps remaining after SA filling")
    return(df |> select(-is_gap, -gap_id, -original_flag, -stage_for_gaps))
  }

  # Add stage context at gap boundaries for event detection
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
                     (!is.na(stage_after)  & stage_after  > event_threshold)
    ) |>
    ungroup()

  fillable   <- gap_info |> filter(gap_mins <= max_gap_mins)
  unfillable <- gap_info |> filter(gap_mins > max_gap_mins)

  message("  ", site, ": ", nrow(fillable), " gaps fillable by spline (",
          nrow(gap_info |> filter(gap_mins <= max_gap_mins & is_event_gap)),
          " during events), ",
          nrow(unfillable), " gaps too long -- left as NA")

  fillable_gap_ids <- fillable$gap_id

  df <- df |>
    mutate(
      fill_eligible    = is_gap & (gap_id %in% fillable_gap_ids),
      stage_for_spline = if_else(fill_eligible | !is_gap, stage_for_gaps, NA_real_),
      stage_splined    = na.spline(stage_for_spline, na.rm = FALSE)
    )

  # Assign flags -- distinguish between gap fills and replacements
  df <- df |>
    left_join(
      gap_info |> select(gap_id, gap_mins, is_event_gap, any_bad_data),
      by = "gap_id"
    ) |>
    mutate(
      qc_flag = case_when(
        # Fillable gaps -- bad_data origin
        is_gap & gap_mins <= max_gap_mins & is_event_gap  & any_bad_data ~ "replaced_spline_event",
        is_gap & gap_mins <= max_gap_mins & !is_event_gap & any_bad_data ~ "replaced_spline",
        # Fillable gaps -- transmission gap origin
        is_gap & gap_mins <= max_gap_mins & is_event_gap                 ~ "gf_spline_event",
        is_gap & gap_mins <= max_gap_mins & !is_event_gap                ~ "gf_spline",
        # Unfillable -- retain original flag (bad_data stays bad_data, NA stays unfilled)
        is_gap & gap_mins > max_gap_mins & any_bad_data                  ~ "bad_data",
        is_gap & gap_mins > max_gap_mins                                 ~ "unfilled",
        TRUE ~ qc_flag
      ),
      stage_qc = case_when(
        qc_flag %in% c("gf_spline", "gf_spline_event",
                        "replaced_spline", "replaced_spline_event") ~ stage_splined,
        TRUE ~ stage_qc
      )
    ) |>
    select(-is_gap, -gap_id, -gap_mins, -is_event_gap, -any_bad_data,
           -stage_splined, -fill_eligible, -stage_for_spline,
           -original_flag, -stage_for_gaps)

  return(df)
}

# Apply spline filling to each primary sensor
primary <- primary |>
  group_by(site_id) |>
  group_modify(~ fill_with_spline(.x, MAX_FILL_GAP_MINS, EVENT_STAGE_THRESHOLD)) |>
  ungroup()


# -----------------------------------------------------------------------------
# 6. QC summary
# -----------------------------------------------------------------------------

message("\n--- QC flag summary ---")
primary |>
  count(site_id, qc_flag) |>
  pivot_wider(names_from = qc_flag, values_from = n, values_fill = 0) |>
  print()


# -----------------------------------------------------------------------------
# 7. Validation plots
# -----------------------------------------------------------------------------
# One page per primary sensor showing:
#   - Full record with points coloured by QC flag
#   - Allows visual verification that flags look reasonable

flag_colours <- c(
  "raw"                    = "grey60",
  "bad_data"               = "#E41A1C",
  "spike"                  = "#FF7F00",
  "replaced_sa"            = "#4DAF4A",
  "replaced_spline"        = "#377EB8",
  "replaced_spline_event"  = "#984EA3",
  "gf_sa"                  = "#A6D96A",
  "gf_spline"              = "#74C2E1",
  "gf_spline_event"        = "#C994C7",
  "unfilled"               = "black"
)

plot_qc_sensor <- function(df, site) {

  df |>
    filter(site_id == site) |>
    ggplot(aes(x = timestamp, y = stage_qc, colour = qc_flag)) +
    geom_point(size = 0.3, na.rm = TRUE) +
    scale_colour_manual(values = flag_colours, drop = FALSE) +
    scale_x_datetime(date_breaks = "6 months", date_labels = "%b %Y") +
    labs(
      title   = paste0("SSN703 -- QC flags: ", site),
      x       = NULL,
      y       = "Stage QC (m)",
      colour  = "QC flag",
      caption = paste0(
        "Spike threshold: ", SPIKE_RATE_M_PER_5MIN, " m per 5min | ",
        "Max spline fill: ", MAX_FILL_GAP_MINS, " min | ",
        "SA R2 threshold: ", SA_R2_THRESHOLD, "\n",
        "Plot generated by 04_stage_qc.R"
      )
    ) +
    theme_bw() +
    theme(
      legend.position  = "bottom",
      panel.grid.minor = element_blank(),
      plot.caption     = element_text(hjust = 0, size = 8, colour = "grey40")
    )
}

out_path <- file.path(plot_dir, "ssn703_qc_summary.pdf")
pdf(out_path, width = 14, height = 6)
for (site in unique(primary$site_id)) {
  print(plot_qc_sensor(primary, site))
}
dev.off()

message("\nSaved: ", out_path)


# -----------------------------------------------------------------------------
# 8. Save QC output
# -----------------------------------------------------------------------------

# Add SA sensor back to the output for completeness
stage_qc <- bind_rows(
  primary,
  stage |> filter(site_id == "ssn703_sa")
)

saveRDS(stage_qc, file.path(data_dir, "ssn703_qc.rds"))

message("Saved: ", file.path(data_dir, "ssn703_qc.rds"))
message("\nNext steps:")
message("  1. Open ssn703_qc_summary.pdf and check flags look reasonable")
message("  2. If spike threshold is too aggressive or too lenient, adjust")
message("     SPIKE_RATE_M_PER_5MIN at the top of this script and rerun")
message("  3. If gap fill decisions look wrong, adjust MAX_FILL_GAP_MINS")
message("     or EVENT_STAGE_THRESHOLD and rerun")
message("  4. Proceed to 05_stage_output.R")
