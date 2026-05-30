# =============================================================================
# SSN844 Stage QC - Script 04: Spike Detection, Flagging, and Gap Filling
# =============================================================================
# Purpose:
#   Apply quality control to the SSN844 stage record:
#   1. Trim to deployment window
#   2. Flag known spike periods:
#        Spike 1: 2023-06-05 to 2023-06-11 (sharp spike)
#        Spike 2: 2023-07-27 to 2023-07-30 (sharp spike)
#        Spike 3: 2021-06-27 to 2021-07-19 (slow drift/blockage, missed by algorithm)
#   3. Spike detection (rate of change + rolling median)
#   4. Gap filling -- Tier 1: SA sensor relationship
#   5. Gap filling -- Tier 2/3: Spline interpolation
#   6. Output clean stage file
#
# Flags applied:
#   "raw"                   -- original value, no QC applied
#   "bad_data"              -- confirmed bad period; unfillable after all tiers
#   "spike"                 -- detected as spike; value set to NA
#   "replaced_sa"           -- bad_data or spike replaced using SA relationship
#   "replaced_spline"       -- bad_data or spike replaced using spline
#   "replaced_spline_event" -- as above but during event conditions
#   "gf_sa"                 -- transmission gap filled using SA relationship
#   "gf_spline"             -- transmission gap filled using spline (baseflow)
#   "gf_spline_event"       -- transmission gap filled using spline (event)
#   "unfilled"              -- gap too long, no SA coverage; remains NA
#
# Inputs:
#   02_processing/data_parsed/ssn844_all_raw.rds
#
# Outputs:
#   02_processing/data_output/per_rc/ssn844_RC1_stage_qc.csv
#   02_processing/data_output/per_rc/ssn844_RC2_stage_qc.csv
#   02_processing/plots/ssn844_qc_summary.pdf
#
# Notes:
#   - Single primary sensor location throughout -- no overlap handling needed
#   - RC1: Rating 1 (2012-01-01 to 2017-07-12)
#   - RC2: Rating 2 (2017-07-13 onward)
#   - Spike periods hard-coded from visual inspection in 02_inspect_stage_844.R:
#       Spike 1: 2023-06-05 to 2023-06-11 (sharp spike)
#       Spike 2: 2023-07-27 to 2023-07-30 (sharp spike)
#       Spike 3: 2021-06-27 to 2021-07-19 (slow drift/blockage)
#   - Adjust QC parameters at top of script if needed; do not change inline
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)
library(zoo)

select <- dplyr::select

# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

plot_dir   <- "02_processing/plots"
data_dir   <- "02_processing/data_parsed"
out_rc_dir <- "02_processing/data_output/per_rc"

dir.create(plot_dir,   showWarnings = FALSE, recursive = TRUE)
dir.create(out_rc_dir, showWarnings = FALSE, recursive = TRUE)

# QC parameters -- adjust here, not inline
SPIKE_RATE_M_PER_5MIN <- 0.03   # max plausible stage change in 5 minutes (m)
SPIKE_WINDOW           <- 5     # rolling median window (timesteps)
MAX_FILL_GAP_MINS      <- 180   # maximum gap to spline fill without SA (minutes)
SA_R2_THRESHOLD        <- 0.95  # minimum R2 to use SA for gap filling
EVENT_STAGE_THRESHOLD  <- 0.60  # stage above which conditions are event-like

# Known spike periods -- confirmed from 02_inspect_stage_844.R
SPIKE_1_START <- as.POSIXct("2023-06-05 00:00:00", tz = "Etc/GMT+8")
SPIKE_1_END   <- as.POSIXct("2023-06-11 23:55:00", tz = "Etc/GMT+8")

SPIKE_2_START <- as.POSIXct("2023-07-27 10:50:00", tz = "Etc/GMT+8")
SPIKE_2_END   <- as.POSIXct("2023-07-30 17:35:00", tz = "Etc/GMT+8")

# Slow drift/blockage -- gradual climb from ~0.35m to ~1.0m, missed by
# rate-of-change algorithm; confirmed from plotly inspection of raw vs QC'd
SPIKE_3_START <- as.POSIXct("2021-06-27 12:45:00", tz = "Etc/GMT+8")
SPIKE_3_END   <- as.POSIXct("2021-07-19 11:00:00", tz = "Etc/GMT+8")

# Rating curve period boundary
RC2_START <- as.POSIXct("2017-07-13 00:00:00", tz = "Etc/GMT+8")

# Output columns
output_cols <- c(
  "station_id", "site_id", "location_id", "rating_curve_period",
  "timestamp", "water_year",
  "stage_corrected", "offset_applied", "qc_flag"
)

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

# -----------------------------------------------------------------------------
# 1. Load data
# -----------------------------------------------------------------------------

stage <- readRDS(file.path(data_dir, "ssn844_all_raw.rds"))

# Diagnostic: confirm both sensors loaded
message("--- Sensor inventory on load ---")
stage |>
  group_by(site_id, sensor_role) |>
  summarise(
    n             = n(),
    start         = min(timestamp, na.rm = TRUE),
    end           = max(timestamp, na.rm = TRUE),
    n_missing_avg = sum(is.na(stage_avg)),
    .groups       = "drop"
  ) |>
  print()

primary <- stage |>
  filter(sensor_role == "primary") |>
  arrange(timestamp)

sa <- stage |>
  filter(site_id == "ssn844_sa") |>
  select(timestamp, stage_sa = stage_avg) |>
  arrange(timestamp)

message("\nPrimary records: ", nrow(primary))
message("SA records:      ", nrow(sa))

if (nrow(sa) == 0) {
  warning("SA sensor returned 0 rows -- check sensor_registry sensor_role for ssn844_sa")
}

# -----------------------------------------------------------------------------
# 2. Trim to deployment window
# -----------------------------------------------------------------------------

n_before <- nrow(primary)

primary <- primary |>
  filter(
    timestamp >= date_start,
    is.na(date_end) | timestamp <= date_end
  )

message("\nRows trimmed outside deployment window: ", n_before - nrow(primary))
message("Rows remaining after trim: ", nrow(primary))

# -----------------------------------------------------------------------------
# 3. Flag known spike periods and bad data from metadata
# -----------------------------------------------------------------------------

primary <- primary |>
  mutate(
    qc_flag = case_when(
      # Known spike periods -- confirmed from visual inspection
      timestamp >= SPIKE_1_START & timestamp <= SPIKE_1_END ~ "bad_data",
      timestamp >= SPIKE_2_START & timestamp <= SPIKE_2_END ~ "bad_data",
      timestamp >= SPIKE_3_START & timestamp <= SPIKE_3_END ~ "bad_data",
      # Any additional bad data periods from sensor_registry
      !is.na(bad_data_start) & timestamp >= bad_data_start &
        (is.na(bad_data_end) | timestamp <= bad_data_end)  ~ "bad_data",
      TRUE ~ "raw"
    ),
    stage_qc = if_else(qc_flag == "bad_data", NA_real_, stage_avg)
  )

message("\nBad data flagged: ", sum(primary$qc_flag == "bad_data"), " rows")
message("  Spike 1 (Jun 05-11 2023): ",
        sum(primary$timestamp >= SPIKE_1_START & primary$timestamp <= SPIKE_1_END))
message("  Spike 2 (Jul 27-30 2023): ",
        sum(primary$timestamp >= SPIKE_2_START & primary$timestamp <= SPIKE_2_END))

# -----------------------------------------------------------------------------
# 4. Spike detection (rate of change + rolling median)
# -----------------------------------------------------------------------------

primary <- primary |>
  arrange(timestamp) |>
  mutate(
    stage_diff  = abs(stage_qc - lag(stage_qc)),
    rolling_med = rollmedian(stage_qc, k = SPIKE_WINDOW, fill = NA, align = "center"),
    rolling_dev = abs(stage_qc - rolling_med),
    is_spike    = qc_flag == "raw" &
                  (stage_diff > SPIKE_RATE_M_PER_5MIN |
                   rolling_dev > 3 * SPIKE_RATE_M_PER_5MIN),
    qc_flag     = if_else(is_spike & !is.na(is_spike), "spike", qc_flag),
    stage_qc    = if_else(qc_flag == "spike", NA_real_, stage_qc)
  ) |>
  select(-stage_diff, -rolling_med, -rolling_dev, -is_spike)

message("Additional spikes detected by algorithm: ", sum(primary$qc_flag == "spike"), " rows")

# -----------------------------------------------------------------------------
# 5. Gap filling -- Tier 1: SA sensor relationship
# -----------------------------------------------------------------------------

# Diagnostic: check SA join will work before joining
message("\n--- SA sensor diagnostic ---")
message("SA timestamp range: ",
        format(min(sa$timestamp, na.rm = TRUE)), " to ",
        format(max(sa$timestamp, na.rm = TRUE)))
message("SA non-NA rows: ", sum(!is.na(sa$stage_sa)))

primary <- primary |>
  left_join(sa, by = "timestamp")

# Check how many primary rows have SA coverage
n_sa_coverage <- sum(!is.na(primary$stage_sa))
message("Primary rows with SA coverage after join: ", n_sa_coverage,
        " (", round(n_sa_coverage / nrow(primary) * 100, 1), "%)")

if (n_sa_coverage == 0) {
  warning("No SA coverage found after join -- check timestamps align between primary and SA")
  warning("Primary tz: ", format(primary$timestamp[1]))
  warning("SA tz: ", format(sa$timestamp[1]))
}

# Fit SA relationship on clean overlapping data
fit_data <- primary |>
  filter(qc_flag == "raw", !is.na(stage_qc), !is.na(stage_sa))

message("Clean overlapping rows for SA fit: ", nrow(fit_data))

if (nrow(fit_data) >= 100) {

  fit <- lm(stage_qc ~ stage_sa, data = fit_data)
  r2  <- summary(fit)$r.squared
  message("SA relationship R2 = ", round(r2, 4))
  message("SA fit coefficients:")
  print(coef(fit))

  if (r2 >= SA_R2_THRESHOLD) {

    primary <- primary |>
      mutate(
        stage_sa_pred = predict(fit, newdata = data.frame(stage_sa = stage_sa)),
        fill_flag = case_when(
          qc_flag %in% c("bad_data", "spike") & !is.na(stage_sa_pred) ~ "replaced_sa",
          is.na(stage_qc) & !is.na(stage_sa_pred)                      ~ "gf_sa",
          TRUE ~ NA_character_
        ),
        qc_flag  = if_else(!is.na(fill_flag), fill_flag, qc_flag),
        stage_qc = if_else(!is.na(fill_flag), stage_sa_pred, stage_qc)
      ) |>
      select(-stage_sa_pred, -fill_flag)

    message("SA gap filling applied")
    message("  replaced_sa: ", sum(primary$qc_flag == "replaced_sa"))
    message("  gf_sa:       ", sum(primary$qc_flag == "gf_sa"))

  } else {
    message("SA R2 below threshold (", SA_R2_THRESHOLD, ") -- SA gap filling skipped")
  }

} else {
  message("Insufficient clean overlapping data for SA fit -- SA gap filling skipped")
}

primary <- primary |> select(-stage_sa)

# -----------------------------------------------------------------------------
# 6. Gap filling -- Tiers 2/3: Spline interpolation
# -----------------------------------------------------------------------------

primary <- primary |>
  arrange(timestamp) |>
  mutate(
    stage_for_gaps = if_else(qc_flag %in% c("bad_data", "spike"),
                             NA_real_, stage_qc),
    is_gap = is.na(stage_for_gaps),
    gap_id = cumsum(!is_gap & lag(!is_gap, default = TRUE))
  )

gap_info <- primary |>
  filter(is_gap) |>
  group_by(gap_id) |>
  summarise(
    gap_start    = min(timestamp),
    gap_end      = max(timestamp),
    gap_mins     = as.numeric(difftime(max(timestamp), min(timestamp), units = "mins")),
    any_bad_data = any(qc_flag == "bad_data"),
    .groups      = "drop"
  )

message("\nGaps remaining before spline fill: ", nrow(gap_info))
message("Gap size distribution (minutes):")
print(summary(gap_info$gap_mins))

# Add event context at gap boundaries
gap_info <- gap_info |>
  rowwise() |>
  mutate(
    stage_before = primary |>
      filter(timestamp < gap_start, !is.na(stage_for_gaps)) |>
      slice_tail(n = 1) |>
      pull(stage_for_gaps) |>
      (\(x) if (length(x) == 0) NA_real_ else x)(),
    stage_after = primary |>
      filter(timestamp > gap_end, !is.na(stage_for_gaps)) |>
      slice_head(n = 1) |>
      pull(stage_for_gaps) |>
      (\(x) if (length(x) == 0) NA_real_ else x)(),
    is_event_gap = (!is.na(stage_before) & stage_before > EVENT_STAGE_THRESHOLD) |
                   (!is.na(stage_after)  & stage_after  > EVENT_STAGE_THRESHOLD)
  ) |>
  ungroup()

fillable_ids <- gap_info |>
  filter(gap_mins <= MAX_FILL_GAP_MINS) |>
  pull(gap_id)

primary <- primary |>
  mutate(fill_eligible = is_gap & (gap_id %in% fillable_ids)) |>
  mutate(
    stage_for_spline = if_else(fill_eligible | !is_gap, stage_for_gaps, NA_real_),
    stage_splined    = na.spline(stage_for_spline, na.rm = FALSE)
  ) |>
  left_join(
    gap_info |> select(gap_id, gap_mins, is_event_gap, any_bad_data),
    by = "gap_id"
  ) |>
  mutate(
    qc_flag = case_when(
      is_gap & gap_mins <= MAX_FILL_GAP_MINS &  is_event_gap &  any_bad_data ~ "replaced_spline_event",
      is_gap & gap_mins <= MAX_FILL_GAP_MINS & !is_event_gap &  any_bad_data ~ "replaced_spline",
      is_gap & gap_mins <= MAX_FILL_GAP_MINS &  is_event_gap                 ~ "gf_spline_event",
      is_gap & gap_mins <= MAX_FILL_GAP_MINS & !is_event_gap                 ~ "gf_spline",
      is_gap & gap_mins >  MAX_FILL_GAP_MINS &  any_bad_data                 ~ "bad_data",
      is_gap & gap_mins >  MAX_FILL_GAP_MINS                                 ~ "unfilled",
      TRUE ~ qc_flag
    ),
    stage_qc = case_when(
      qc_flag %in% c("gf_spline", "gf_spline_event",
                      "replaced_spline", "replaced_spline_event") ~ stage_splined,
      TRUE ~ stage_qc
    )
  ) |>
  select(-is_gap, -gap_id, -gap_mins, -is_event_gap, -any_bad_data,
         -stage_splined, -fill_eligible, -stage_for_spline, -stage_for_gaps)

# -----------------------------------------------------------------------------
# 7. QC summary
# -----------------------------------------------------------------------------

message("\n--- QC flag summary ---")
primary |>
  count(qc_flag) |>
  mutate(pct = round(n / sum(n) * 100, 2)) |>
  print()

message("Remaining NAs: ", sum(is.na(primary$stage_qc)))

# -----------------------------------------------------------------------------
# 8. Validation plot
# -----------------------------------------------------------------------------

p_qc <- primary |>
  ggplot(aes(x = timestamp, y = stage_qc, colour = qc_flag)) +
  geom_point(size = 0.3, na.rm = TRUE, alpha = 0.6) +
  scale_colour_manual(values = flag_colours, drop = FALSE) +
  scale_x_datetime(date_breaks = "1 year", date_labels = "%Y") +
  labs(
    title   = "SSN844 -- QC flags: ssn844",
    x       = NULL,
    y       = "Stage QC (m)",
    colour  = "QC flag",
    caption = paste0(
      "Spike threshold: ", SPIKE_RATE_M_PER_5MIN, " m per 5min | ",
      "Max spline fill: ", MAX_FILL_GAP_MINS, " min | ",
      "SA R2 threshold: ", SA_R2_THRESHOLD, "\n",
      "Known spikes: Jun 05-11 2023, Jul 27-30 2023\n",
      "Plot generated by 04_stage_qc_844.R"
    )
  ) +
  theme_bw() +
  theme(
    legend.position  = "bottom",
    panel.grid.minor = element_blank(),
    plot.caption     = element_text(hjust = 0, size = 8, colour = "grey40")
  )

pdf(file.path(plot_dir, "ssn844_qc_summary.pdf"), width = 14, height = 6)
print(p_qc)
dev.off()

message("Saved: ", file.path(plot_dir, "ssn844_qc_summary.pdf"))

# -----------------------------------------------------------------------------
# 9. Assemble output and split by rating curve period
# -----------------------------------------------------------------------------

stage_out <- primary |>
  mutate(
    station_id          = "SSN844",
    site_id             = "ssn844",
    location_id         = "loc_1",
    rating_curve_period = if_else(timestamp >= RC2_START, "RC2", "RC1"),
    offset_applied      = 0,
    stage_corrected     = stage_qc
  ) |>
  select(all_of(output_cols))

rc1_out <- stage_out |> filter(rating_curve_period == "RC1")
rc2_out <- stage_out |> filter(rating_curve_period == "RC2")

write_csv(rc1_out, file.path(out_rc_dir, "ssn844_RC1_stage_qc.csv"))
write_csv(rc2_out, file.path(out_rc_dir, "ssn844_RC2_stage_qc.csv"))

message("\n--- Output summary ---")
message("RC1: ", nrow(rc1_out), " rows | ",
        format(min(rc1_out$timestamp)), " to ", format(max(rc1_out$timestamp)))
message("RC2: ", nrow(rc2_out), " rows | ",
        format(min(rc2_out$timestamp)), " to ", format(max(rc2_out$timestamp)))
message("\nQC flag breakdown:")
stage_out |>
  count(rating_curve_period, qc_flag) |>
  pivot_wider(names_from = qc_flag, values_from = n, values_fill = 0) |>
  print()

message("\nDone -- proceed to rating curve discharge computation")
message("  RC1 input: ", file.path(out_rc_dir, "ssn844_RC1_stage_qc.csv"))
message("  RC2 input: ", file.path(out_rc_dir, "ssn844_RC2_stage_qc.csv"))
