# =============================================================================
# SSN844 Stage QC - Script 02: Visual Inspection of Raw Stage Data
# =============================================================================
# Purpose:
#   Generate a multi-page PDF of plots for visual examination of raw stage
#   data for SSN844. This is a human step -- look at the plots, confirm the
#   spike periods, and check for any additional issues before QC.
#
# This script does NOT modify data. It only plots.
#
# Inputs:
#   02_processing/data_parsed/ssn844_all_raw.rds
#
# Outputs:
#   02_processing/plots/ssn844_stage_inspection.pdf
#
# PDF contents:
#   Page 1 -- full record, primary sensor (ssn844), hourly thinned
#   Page 2 -- full record, both sensors overlaid, hourly thinned
#   Page 3 -- spike 1 zoom: 2023-05-25 to 2023-06-21
#   Page 4 -- spike 2 zoom: 2023-07-17 to 2023-08-09
#   Page 5 -- primary vs SA timeseries, full SA deployment period
#   Page 6 -- primary vs SA scatter (linear relationship assessment)
#
# What to look for:
#   - Confirm spike 1 (2023-06-05 to 2023-06-11) bounds look correct
#   - Confirm spike 2 (2023-07-27 to 2023-07-30) bounds look correct
#   - Any other spikes, flatlines, or erratic behaviour
#   - Whether SA sensor tracks primary well enough for gap filling (page 6)
#   - Any power outage gaps visible in the record
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)

# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

plot_dir <- "02_processing/plots"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)

sensor_colours <- c(
  "ssn844"    = "#377EB8",
  "ssn844_sa" = "#FF7F00"
)

sensor_labels <- c(
  "ssn844"    = "ssn844 (primary, loc_1, 2014-present)",
  "ssn844_sa" = "ssn844_sa (supplementary, loc_2)"
)

SPIKE_1_START <- as.POSIXct("2023-06-05", tz = "Etc/GMT+8")
SPIKE_1_END   <- as.POSIXct("2023-06-11", tz = "Etc/GMT+8")

SPIKE_2_START <- as.POSIXct("2023-07-27 10:50:00", tz = "Etc/GMT+8")
SPIKE_2_END   <- as.POSIXct("2023-07-30 17:35:00", tz = "Etc/GMT+8")

# Slow drift/blockage confirmed from plotly inspection
SPIKE_3_START <- as.POSIXct("2021-06-27 12:45:00", tz = "Etc/GMT+8")
SPIKE_3_END   <- as.POSIXct("2021-07-19 11:00:00", tz = "Etc/GMT+8")

theme_stage <- function() {
  theme_bw() +
    theme(
      legend.position  = "bottom",
      legend.title     = element_blank(),
      panel.grid.minor = element_blank(),
      plot.caption     = element_text(hjust = 0, size = 8, colour = "grey40")
    )
}

# -----------------------------------------------------------------------------
# 1. Load data
# -----------------------------------------------------------------------------

stage <- readRDS("02_processing/data_parsed/ssn844_all_raw.rds")

message("Loaded ", nrow(stage), " rows across ", n_distinct(stage$site_id), " sensors")

# Diagnostic: confirm both sensors present with valid stage_avg
message("\n--- Sensor diagnostic ---")
stage |>
  group_by(site_id, sensor_role) |>
  summarise(
    n             = n(),
    start         = min(timestamp, na.rm = TRUE),
    end           = max(timestamp, na.rm = TRUE),
    n_valid_avg   = sum(!is.na(stage_avg)),
    pct_valid     = round(sum(!is.na(stage_avg)) / n() * 100, 1),
    .groups       = "drop"
  ) |>
  print()

# Thin to hourly for full-record plots
stage_hourly <- stage |>
  mutate(timestamp_hour = floor_date(timestamp, "hour")) |>
  group_by(site_id, timestamp_hour) |>
  summarise(stage_avg = mean(stage_avg, na.rm = TRUE), .groups = "drop") |>
  rename(timestamp = timestamp_hour)

message("\nHourly thinned rows: ", nrow(stage_hourly))
message("ssn844 hourly:    ", sum(stage_hourly$site_id == "ssn844"))
message("ssn844_sa hourly: ", sum(stage_hourly$site_id == "ssn844_sa"))

# -----------------------------------------------------------------------------
# 2. Build plots
# -----------------------------------------------------------------------------

# -- Page 1: Full record, primary sensor only ---------------------------------

p1 <- stage_hourly |>
  filter(site_id == "ssn844") |>
  ggplot(aes(x = timestamp, y = stage_avg)) +
  geom_line(colour = sensor_colours["ssn844"], linewidth = 0.3, na.rm = TRUE) +
  annotate("rect",
    xmin = SPIKE_1_START, xmax = SPIKE_1_END,
    ymin = -Inf, ymax = Inf, fill = "red", alpha = 0.15
  ) +
  annotate("rect",
    xmin = SPIKE_2_START, xmax = SPIKE_2_END,
    ymin = -Inf, ymax = Inf, fill = "red", alpha = 0.15
  ) +
  annotate("rect",
    xmin = SPIKE_3_START, xmax = SPIKE_3_END,
    ymin = -Inf, ymax = Inf, fill = "red", alpha = 0.15
  ) +
  annotate("text",
    x = SPIKE_1_START, y = Inf,
    label = "Spike 1\n(Jun 05-11)", hjust = 0, vjust = 1.5, size = 3, colour = "red"
  ) +
  annotate("text",
    x = SPIKE_2_START, y = Inf,
    label = "Spike 2\n(Jul 27-30)", hjust = 0, vjust = 1.5, size = 3, colour = "red"
  ) +
  annotate("text",
    x = SPIKE_3_START, y = Inf,
    label = "Spike 3\n(Jun27-Jul19\n2021)", hjust = 0, vjust = 1.5, size = 3, colour = "red"
  ) +
  scale_x_datetime(date_breaks = "1 year", date_labels = "%Y") +
  labs(
    title   = "SSN844 -- Full stage record, primary sensor (hourly thinned)",
    x       = NULL,
    y       = "Stage (m)",
    caption = "Red shading = known spike periods\nPlot generated by 02_inspect_stage_844.R"
  ) +
  theme_stage()


# -- Page 2: Full record, both sensors overlaid -------------------------------

p2 <- stage_hourly |>
  ggplot(aes(x = timestamp, y = stage_avg, colour = site_id)) +
  geom_line(linewidth = 0.3, na.rm = TRUE, alpha = 0.8) +
  annotate("rect",
    xmin = SPIKE_1_START, xmax = SPIKE_1_END,
    ymin = -Inf, ymax = Inf, fill = "red", alpha = 0.1
  ) +
  annotate("rect",
    xmin = SPIKE_2_START, xmax = SPIKE_2_END,
    ymin = -Inf, ymax = Inf, fill = "red", alpha = 0.1
  ) +
  annotate("rect",
    xmin = SPIKE_3_START, xmax = SPIKE_3_END,
    ymin = -Inf, ymax = Inf, fill = "red", alpha = 0.1
  ) +
  scale_colour_manual(values = sensor_colours, labels = sensor_labels) +
  scale_x_datetime(date_breaks = "1 year", date_labels = "%Y") +
  labs(
    title   = "SSN844 -- Full stage record, both sensors overlaid (hourly thinned)",
    x       = NULL,
    y       = "Stage (m)",
    caption = "Red shading = known spike periods\nPlot generated by 02_inspect_stage_844.R"
  ) +
  theme_stage()


# -- Page 3: Spike 1 zoom -----------------------------------------------------

p3 <- stage |>
  filter(
    site_id == "ssn844",
    timestamp >= SPIKE_1_START - days(10),
    timestamp <= SPIKE_1_END   + days(10)
  ) |>
  ggplot(aes(x = timestamp, y = stage_avg)) +
  geom_line(colour = sensor_colours["ssn844"], linewidth = 0.4, na.rm = TRUE) +
  annotate("rect",
    xmin = SPIKE_1_START, xmax = SPIKE_1_END,
    ymin = -Inf, ymax = Inf, fill = "red", alpha = 0.15
  ) +
  scale_x_datetime(date_breaks = "4 days", date_labels = "%d %b") +
  labs(
    title    = "SSN844 -- Spike 1 zoom (±10 days around 2023-06-05 to 06-11)",
    subtitle = "5min resolution | confirm spike bounds look correct",
    x        = NULL,
    y        = "Stage (m)",
    caption  = "Red shading = spike period flagged in 04_stage_qc_844.R\nPlot generated by 02_inspect_stage_844.R"
  ) +
  theme_stage()


# -- Page 4: Spike 2 zoom -----------------------------------------------------

p4 <- stage |>
  filter(
    site_id == "ssn844",
    timestamp >= SPIKE_2_START - days(10),
    timestamp <= SPIKE_2_END   + days(10)
  ) |>
  ggplot(aes(x = timestamp, y = stage_avg)) +
  geom_line(colour = sensor_colours["ssn844"], linewidth = 0.4, na.rm = TRUE) +
  annotate("rect",
    xmin = SPIKE_2_START, xmax = SPIKE_2_END,
    ymin = -Inf, ymax = Inf, fill = "red", alpha = 0.15
  ) +
  scale_x_datetime(date_breaks = "4 days", date_labels = "%d %b") +
  labs(
    title    = "SSN844 -- Spike 2 zoom (±10 days around 2023-07-27 to 07-30)",
    subtitle = "5min resolution | confirm spike bounds look correct",
    x        = NULL,
    y        = "Stage (m)",
    caption  = "Red shading = spike period flagged in 04_stage_qc_844.R\nPlot generated by 02_inspect_stage_844.R"
  ) +
  theme_stage()


# -- Page 5: Spike 3 zoom (slow drift 2021) -----------------------------------

p5_spike3 <- stage |>
  filter(
    site_id == "ssn844",
    timestamp >= SPIKE_3_START - days(10),
    timestamp <= SPIKE_3_END   + days(10)
  ) |>
  ggplot(aes(x = timestamp, y = stage_avg)) +
  geom_line(colour = sensor_colours["ssn844"], linewidth = 0.4, na.rm = TRUE) +
  annotate("rect",
    xmin = SPIKE_3_START, xmax = SPIKE_3_END,
    ymin = -Inf, ymax = Inf, fill = "red", alpha = 0.15
  ) +
  scale_x_datetime(date_breaks = "4 days", date_labels = "%d %b") +
  labs(
    title    = "SSN844 -- Spike 3 zoom (±10 days around 2021-06-27 to 07-19)",
    subtitle = "5min resolution | slow drift/blockage -- missed by rate-of-change algorithm",
    x        = NULL,
    y        = "Stage (m)",
    caption  = "Red shading = spike period flagged in 04_stage_qc_844.R\nPlot generated by 02_inspect_stage_844.R"
  ) +
  theme_stage()


# -- Page 6: Primary vs SA timeseries, full SA deployment period --------------

sa_start <- stage |>
  filter(site_id == "ssn844_sa", !is.na(stage_avg)) |>
  summarise(start = min(timestamp, na.rm = TRUE)) |>
  pull(start)

sa_end <- stage |>
  filter(site_id == "ssn844_sa", !is.na(stage_avg)) |>
  summarise(end = max(timestamp, na.rm = TRUE)) |>
  pull(end)

message("\nSA deployment range with valid data: ",
        format(sa_start), " to ", format(sa_end))

p5 <- stage_hourly |>
  filter(timestamp >= sa_start, timestamp <= sa_end) |>
  ggplot(aes(x = timestamp, y = stage_avg, colour = site_id)) +
  geom_line(linewidth = 0.3, na.rm = TRUE, alpha = 0.8) +
  scale_colour_manual(values = sensor_colours, labels = sensor_labels) +
  scale_x_datetime(date_breaks = "6 months", date_labels = "%b %Y") +
  labs(
    title    = "SSN844 -- Primary vs SA sensor during SA deployment (hourly thinned)",
    subtitle = "Use to assess SA gap fill suitability -- sensors should track each other well",
    x        = NULL,
    y        = "Stage (m)",
    caption  = "Plot generated by 02_inspect_stage_844.R"
  ) +
  theme_stage()


# -- Page 6: Primary vs SA scatter --------------------------------------------

scatter_data <- stage |>
  filter(!is.na(stage_avg)) |>
  select(timestamp, site_id, stage_avg) |>
  pivot_wider(names_from = site_id, values_from = stage_avg) |>
  filter(!is.na(ssn844), !is.na(ssn844_sa))

message("Clean overlapping rows for scatter: ", nrow(scatter_data))

if (nrow(scatter_data) > 10) {

  fit_scatter <- lm(ssn844 ~ ssn844_sa, data = scatter_data)
  r2_scatter  <- round(summary(fit_scatter)$r.squared, 4)
  message("SA scatter R2: ", r2_scatter)

  p6 <- scatter_data |>
    ggplot(aes(x = ssn844_sa, y = ssn844)) +
    geom_point(size = 0.3, alpha = 0.3, colour = "grey40") +
    geom_smooth(method = "lm", colour = "red", se = FALSE, linewidth = 0.8) +
    annotate("text",
      x = -Inf, y = Inf,
      label = paste0("R² = ", r2_scatter),
      hjust = -0.2, vjust = 1.5, size = 4, colour = "red"
    ) +
    labs(
      title   = "SSN844 -- Primary vs SA stage scatter (5min, all overlapping data)",
      subtitle = paste0("R² = ", r2_scatter,
                        " | threshold for gap filling = ", 0.95),
      x       = "ssn844_sa stage (m)",
      y       = "ssn844 stage (m)",
      caption = "Red line = linear fit\nPlot generated by 02_inspect_stage_844.R"
    ) +
    theme_stage()

} else {
  message("Insufficient overlapping data for scatter plot -- check SA sensor data")
  p6 <- ggplot() +
    annotate("text", x = 0.5, y = 0.5,
             label = "Insufficient overlapping data for scatter\nCheck SA sensor data",
             size = 5) +
    theme_void()
}

# -----------------------------------------------------------------------------
# 3. Save PDF
# -----------------------------------------------------------------------------

out_path <- file.path(plot_dir, "ssn844_stage_inspection.pdf")

pdf(out_path, width = 14, height = 8)
print(p1)          # Page 1: full record, primary only
print(p2)          # Page 2: full record, both sensors overlaid
print(p3)          # Page 3: spike 1 zoom (2023-06-05 to 06-11)
print(p4)          # Page 4: spike 2 zoom (2023-07-27 to 07-30)
print(p5_spike3)   # Page 5: spike 3 zoom (2021-06-27 to 07-19, slow drift)
print(p5)          # Page 6: primary vs SA timeseries
print(p6)          # Page 7: primary vs SA scatter
dev.off()

message("\nSaved: ", out_path)
message("\nNext steps:")
message("  1. Open PDF and examine all pages")
message("  2. Confirm spike 1 bounds (page 3) -- adjust SPIKE_1 dates in 04 if needed")
message("  3. Confirm spike 2 bounds (page 4) -- adjust SPIKE_2 dates in 04 if needed")
message("  4. Confirm spike 3 bounds (page 5) -- adjust SPIKE_3 dates in 04 if needed")
message("  5. Check SA scatter R2 (page 7) -- must be >= 0.95 for SA gap filling")
message("  6. Note any additional issues to address in 04_stage_qc_844.R")
message("  7. Proceed to 04_stage_qc_844.R")
