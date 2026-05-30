# =============================================================================
# 07_stage_qaqc_844.R
# Stage QAQC for SSN844
#
# Site: SSN844, loc_1 (single location throughout 2014-present)
# Supplementary sensor: ssn844_sa (loc_2, gap filling only)
#
# QC steps:
#   1. Read primary (ssn844) and supplementary (ssn844_sa) stage data
#   2. Build complete timestamp spine at 300s intervals
#   3. Flag and remove known spike period: 2023-06-05 to 2023-06-11
#   4. Gap fill from ssn844_sa where available
#   5. Spline fill remaining small gaps
#   6. Assign qc_flag, water_year, rating_curve_period
#   7. Save output
#
# No datum corrections required (single sensor location throughout)
# No offset applied (set to 0)
# =============================================================================

library(tidyverse)
library(lubridate)

# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

select <- dplyr::select

TIMESTEP_SEC  <- 300
SPIKE_START   <- as.POSIXct("2023-06-05 00:00:00", tz = "Etc/GMT+8")
SPIKE_END     <- as.POSIXct("2023-06-11 23:55:00", tz = "Etc/GMT+8")
MAX_SPLINE_GAP <- 24  # maximum gap to spline fill, in hours

# Rating curve periods
RC_BREAKS <- as.POSIXct(c(
  "2012-01-01 00:00:00",  # Rating 1 start
  "2017-07-13 00:00:00"   # Rating 2 start
), tz = "Etc/GMT+8")

# Water year function (Oct 1 - Sep 30)
water_year <- function(dt) {
  yr <- year(dt)
  mo <- month(dt)
  wy_start <- ifelse(mo >= 10, yr, yr - 1)
  paste0(wy_start, "-", wy_start + 1)
}

# -----------------------------------------------------------------------------
# 1. Read primary and supplementary data
# -----------------------------------------------------------------------------

primary <- read_csv(
  "02_processing/data/ssn844/ssn844.csv",
  skip = 3,
  col_names = c("timestamp", "year", "month", "water_year_raw",
                "stage_corrected", "stage_avg", "stage_min", "stage_max", "stage_std"),
  col_types = cols(.default = "c")
) |>
  mutate(
    timestamp        = as.POSIXct(timestamp, format = "%m/%d/%Y %H:%M", tz = "Etc/GMT+8"),
    stage_corrected  = as.numeric(stage_corrected)
  ) |>
  filter(!is.na(timestamp)) |>
  arrange(timestamp)

supplementary <- read_csv(
  "02_processing/data/ssn844_sa/ssn844_sa.csv",
  skip = 3,
  col_names = c("timestamp", "year", "month", "water_year_raw",
                "stage_corrected", "stage_avg", "stage_min", "stage_max", "stage_std"),
  col_types = cols(.default = "c")
) |>
  mutate(
    timestamp        = as.POSIXct(timestamp, format = "%m/%d/%Y %H:%M", tz = "Etc/GMT+8"),
    stage_corrected  = as.numeric(stage_corrected)
  ) |>
  filter(!is.na(timestamp)) |>
  arrange(timestamp)

cat("Primary records:      ", nrow(primary), "\n")
cat("Primary range:        ", format(min(primary$timestamp)), "to", format(max(primary$timestamp)), "\n")
cat("Supplementary records:", nrow(supplementary), "\n")
cat("Supplementary range:  ", format(min(supplementary$timestamp)), "to", format(max(supplementary$timestamp)), "\n")

# -----------------------------------------------------------------------------
# 2. Build complete timestamp spine
# -----------------------------------------------------------------------------

ts_start <- floor_date(min(primary$timestamp), "5 minutes")
ts_end   <- floor_date(max(primary$timestamp), "5 minutes")

spine <- tibble(
  timestamp = seq(ts_start, ts_end, by = TIMESTEP_SEC)
)

cat("\nSpine records:", nrow(spine), "\n")

# Join primary data to spine
stage <- spine |>
  left_join(
    primary |> select(timestamp, stage_raw = stage_corrected),
    by = "timestamp"
  )

# -----------------------------------------------------------------------------
# 3. Flag and remove spike period 2023-06-05 to 2023-06-11
# -----------------------------------------------------------------------------

stage <- stage |>
  mutate(
    spike = timestamp >= SPIKE_START & timestamp <= SPIKE_END,
    stage_raw = if_else(spike, NA_real_, stage_raw)
  )

cat("Spike timestamps removed:", sum(stage$spike), "\n")

# -----------------------------------------------------------------------------
# 4. Gap fill from supplementary sensor (ssn844_sa)
# -----------------------------------------------------------------------------

# Join supplementary
stage <- stage |>
  left_join(
    supplementary |> select(timestamp, stage_sa = stage_corrected),
    by = "timestamp"
  )

# Compute offset between sensors during overlap periods where both are valid
offset_df <- stage |>
  filter(!is.na(stage_raw), !is.na(stage_sa)) |>
  mutate(offset = stage_raw - stage_sa)

median_offset <- median(offset_df$offset, na.rm = TRUE)
cat("\nMedian offset (primary - supplementary):", round(median_offset, 5), "m\n")

# Apply offset correction to supplementary and use to fill gaps
stage <- stage |>
  mutate(
    stage_sa_corrected = stage_sa + median_offset,
    # Fill from sa where primary is NA and sa is available
    filled_from_sa = is.na(stage_raw) & !is.na(stage_sa_corrected),
    stage_filled = if_else(filled_from_sa, stage_sa_corrected, stage_raw)
  )

cat("Timestamps filled from ssn844_sa:", sum(stage$filled_from_sa), "\n")

# -----------------------------------------------------------------------------
# 5. Spline fill remaining small gaps
# -----------------------------------------------------------------------------

# Identify remaining gaps
stage <- stage |>
  mutate(still_na = is.na(stage_filled))

# Label contiguous gap runs
stage <- stage |>
  mutate(
    gap_id = cumsum(!still_na | lag(!still_na, default = TRUE))
  )

# Compute gap lengths
gap_lengths <- stage |>
  filter(still_na) |>
  group_by(gap_id) |>
  summarise(gap_n = n(), .groups = "drop") |>
  mutate(gap_hours = gap_n * TIMESTEP_SEC / 3600)

cat("\nRemaining gaps before spline fill:\n")
print(gap_lengths |> arrange(desc(gap_hours)))

# Spline fill gaps up to MAX_SPLINE_GAP hours
large_gap_ids <- gap_lengths |>
  filter(gap_hours > MAX_SPLINE_GAP) |>
  pull(gap_id)

stage <- stage |>
  mutate(
    spline_eligible = still_na & !(gap_id %in% large_gap_ids)
  )

# Apply spline interpolation
stage_vec <- stage$stage_filled
spline_idx <- which(stage$spline_eligible)

if (length(spline_idx) > 0) {
  valid_idx <- which(!is.na(stage_vec))
  stage_vec[spline_idx] <- approx(
    x = valid_idx,
    y = stage_vec[valid_idx],
    xout = spline_idx,
    method = "linear",
    rule = 1
  )$y
}

stage <- stage |>
  mutate(
    stage_final = stage_vec,
    filled_spline = spline_eligible & !is.na(stage_vec)
  )

cat("Timestamps filled by spline:", sum(stage$filled_spline, na.rm = TRUE), "\n")
cat("Remaining NAs after all fill:", sum(is.na(stage$stage_final)), "\n")

# -----------------------------------------------------------------------------
# 6. Assign qc_flag, water_year, rating_curve_period
# -----------------------------------------------------------------------------

stage <- stage |>
  mutate(
    qc_flag = case_when(
      spike                        ~ "removed_spike",
      filled_from_sa               ~ "gf_sa",
      filled_spline                ~ "gf_spline",
      !is.na(stage_raw)            ~ "raw",
      TRUE                         ~ "unfilled"
    ),
    water_year = water_year(timestamp),
    rating_curve_period = case_when(
      timestamp >= RC_BREAKS[2] ~ "RC2",
      timestamp >= RC_BREAKS[1] ~ "RC1",
      TRUE                      ~ NA_character_
    ),
    offset_applied  = 0,
    station_id      = "SSN844",
    location_id     = "loc_1",
    site_id         = "ssn844"
  ) |>
  select(
    station_id, location_id, rating_curve_period, timestamp,
    water_year, stage_corrected = stage_final,
    offset_applied, qc_flag, site_id
  )

# -----------------------------------------------------------------------------
# 7. QC summary plots
# -----------------------------------------------------------------------------

# Full timeseries
p_ts <- ggplot(stage, aes(x = timestamp, y = stage_corrected, colour = qc_flag)) +
  geom_line(data = stage |> filter(qc_flag == "raw"), colour = "black", linewidth = 0.3) +
  geom_point(data = stage |> filter(qc_flag != "raw"), size = 0.5) +
  scale_colour_manual(values = c(
    "raw"            = "black",
    "gf_sa"          = "steelblue",
    "gf_spline"      = "orange",
    "removed_spike"  = "red",
    "unfilled"       = "grey50"
  )) +
  labs(
    title = "SSN844 stage QAQC -- full timeseries",
    x = NULL, y = "Stage (m)", colour = "QC flag"
  ) +
  theme_bw()

print(p_ts)

# Spike period zoom
p_spike <- stage |>
  filter(timestamp >= SPIKE_START - days(5),
         timestamp <= SPIKE_END   + days(5)) |>
  ggplot(aes(x = timestamp, y = stage_corrected, colour = qc_flag)) +
  geom_line(linewidth = 0.5) +
  geom_point(size = 0.8) +
  scale_colour_manual(values = c(
    "raw"            = "black",
    "gf_sa"          = "steelblue",
    "gf_spline"      = "orange",
    "removed_spike"  = "red",
    "unfilled"       = "grey50"
  )) +
  labs(
    title = "SSN844 -- spike period zoom (2023-06-05 to 2023-06-11)",
    x = NULL, y = "Stage (m)", colour = "QC flag"
  ) +
  theme_bw()

print(p_spike)

# QC flag summary
cat("\nQC flag summary:\n")
stage |>
  count(qc_flag) |>
  mutate(pct = round(n / sum(n) * 100, 2)) |>
  print()

# -----------------------------------------------------------------------------
# 8. Save output
# -----------------------------------------------------------------------------

write_csv(stage, "04_outputs/ssn844_stage_qc.csv")

cat("\nDone. QC stage saved to 04_outputs/ssn844_stage_qc.csv\n")
cat("Rows:", nrow(stage), "\n")
cat("Date range:", format(min(stage$timestamp)), "to", format(max(stage$timestamp)), "\n")
