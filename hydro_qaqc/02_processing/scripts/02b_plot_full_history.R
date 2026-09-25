# =============================================================================
# SSN703 Stage QC - Script 02b: Full Sensor History with Rating Curve Periods
# =============================================================================
# Purpose:
#   Single overview plot -- all SSN703 stage sensors layered on one panel,
#   with vertical lines marking where each primary sensor's deployment
#   period ends, and shaded blocks showing which rating curve period is
#   in effect over time.
#
# Inputs:
#   02_processing/data_parsed/ssn703_all_raw.rds
#   03_docs/metadata/sensor_registry.csv
#
# Outputs:
#   02_processing/plots/ssn703_sensor_history_overview.pdf
#   02_processing/plots/ssn703_sensor_history_overview.png
#
# Notes on rating curve period boundaries:
#   sensor_registry's rating_curve_period column tells you which RC each
#   sensor feeds, but does NOT by itself give the *authoritative*
#   start/end date to use for shading -- in practice the discharge
#   pipeline (10_discharge.R) uses database cutover dates that differ
#   from physical sensor install dates:
#     RC1: ssn703_a start -> RC2 cutover (loc_1)              = 2014-08-03 -> 2019-02-09
#     RC2: authoritative from 2019-02-09 (db cutover), NOT
#          ssn703_c's physical install date of 2018-09-14     -- see 10_discharge.R
#     RC3: authoritative from 2023-09-15 (db cutover), NOT
#          ssn703_d's physical install date of 2021-09-02     -- see 10_discharge.R
#   These cutover dates are hardcoded below in `rc_periods`. Edit that
#   table directly if the authoritative dates change -- it is deliberately
#   NOT derived automatically from sensor_registry, to avoid silently
#   baking in the wrong assumption about when a rating curve actually
#   took effect in the database vs. when the sensor was physically
#   installed.
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)

plot_dir <- "02_processing/plots"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)


# -----------------------------------------------------------------------------
# 1. Load data and metadata
# -----------------------------------------------------------------------------

stage <- readRDS("02_processing/data_parsed/ssn703_all_raw.rds")

sensor_registry <- read_csv("03_docs/metadata/sensor_registry.csv",
                             show_col_types = FALSE) |>
  filter(station_id == "SSN703") |>
  mutate(across(c(date_start, date_end, bad_data_start, bad_data_end),
                ~ as.POSIXct(.x, tz = "Etc/GMT+8")))

now_time <- Sys.time()


# -----------------------------------------------------------------------------
# 2. Sensor colours / labels (consistent with 02_inspect_stage.R)
# -----------------------------------------------------------------------------

sensor_colours <- c(
  "ssn703_a"  = "#E41A1C",
  "ssn703_b"  = "#377EB8",
  "ssn703_c"  = "#4DAF4A",
  "ssn703_d"  = "#984EA3",
  "ssn703_sa" = "#FF7F00"
)

sensor_labels <- c(
  "ssn703_a"  = "703a (loc_1, RC1)",
  "ssn703_b"  = "703b (loc_1, RC1)",
  "ssn703_c"  = "703c (loc_2, RC2)",
  "ssn703_d"  = "703d (loc_3, RC3)",
  "ssn703_sa" = "703sa (supplementary)"
)


# -----------------------------------------------------------------------------
# 3. Rating curve period blocks (authoritative -- see notes in header)
# -----------------------------------------------------------------------------

rc_periods <- tribble(
  ~rc_label,                    ~rc_start,                                             ~rc_end,
  "RC1 (loc_1: 703a -> 703b)",  as.POSIXct("2014-08-03 10:55:00", tz = "Etc/GMT+8"),    as.POSIXct("2019-02-09 00:00:00", tz = "Etc/GMT+8"),
  "RC2 (loc_2: 703c)",          as.POSIXct("2019-02-09 00:00:00", tz = "Etc/GMT+8"),    as.POSIXct("2023-09-15 00:00:00", tz = "Etc/GMT+8"),
  "RC3 (loc_3: 703d)",          as.POSIXct("2023-09-15 00:00:00", tz = "Etc/GMT+8"),    now_time
)

rc_fill_colours <- c(
  "RC1 (loc_1: 703a -> 703b)" = "#B3CDE3",
  "RC2 (loc_2: 703c)"         = "#CCEBC5",
  "RC3 (loc_3: 703d)"         = "#DECBE4"
)


# -----------------------------------------------------------------------------
# 4. Sensor deployment-end lines
# -----------------------------------------------------------------------------
# One vertical line per primary sensor's date_end (excludes NA/ongoing
# sensors, and excludes the supplementary sensor -- it doesn't define an
# RC boundary and would just clutter the plot)
#
# IMPORTANT: date_end marks when a sensor's record stops, not when it
# stopped being trustworthy. A sensor can be visibly degrading for weeks
# before date_end, while still nominally "overlapping" with its
# replacement -- that overlap period is not automatically clean or usable.
# See bad_data_periods below, which is what actually flags the untrustworthy
# portion.

sensor_end_lines <- sensor_registry |>
  filter(!is.na(date_end), sensor_role == "primary")


# -----------------------------------------------------------------------------
# 4b. Confirmed bad data periods (the untrustworthy portion of any overlap)
# -----------------------------------------------------------------------------
# bad_data_start/bad_data_end in sensor_registry mark where a given sensor
# is known to be degraded, independent of when its nominal deployment record
# ends. This is what actually determines whether an overlap window is safe
# to use for offset/QC purposes -- not the overlap window itself.
#   e.g. ssn703_a: failed partway through its overlap with ssn703_b
#        ssn703_c: stage went bad ~6 weeks before ssn703_d took over
# "ongoing" bad periods (NA bad_data_end) are drawn through to now_time.

bad_data_periods <- sensor_registry |>
  filter(!is.na(bad_data_start)) |>
  mutate(bad_data_end_plot = if_else(is.na(bad_data_end), now_time, bad_data_end))


# -----------------------------------------------------------------------------
# 5. Thin stage data to hourly for plotting
# -----------------------------------------------------------------------------

stage_hourly <- stage |>
  mutate(timestamp_hour = floor_date(timestamp, "hour")) |>
  group_by(site_id, timestamp_hour) |>
  summarise(stage_avg = mean(stage_avg, na.rm = TRUE), .groups = "drop") |>
  rename(timestamp = timestamp_hour)


# -----------------------------------------------------------------------------
# 6. Build plot
# -----------------------------------------------------------------------------

p <- ggplot() +
  # rating curve period blocks (drawn first, sit behind everything else)
  geom_rect(
    data = rc_periods,
    aes(xmin = rc_start, xmax = rc_end, ymin = -Inf, ymax = Inf, fill = rc_label),
    alpha = 0.35, inherit.aes = FALSE
  ) +
  scale_fill_manual(name = "Rating curve period", values = rc_fill_colours) +
  # confirmed bad data periods -- drawn on top of RC blocks, below the stage
  # lines, so the untrustworthy portion of any nominal overlap is visible
  # as a distinct red band rather than looking like clean concurrent data
  geom_rect(
    data = bad_data_periods,
    aes(xmin = bad_data_start, xmax = bad_data_end_plot, ymin = -Inf, ymax = Inf),
    fill = "red", alpha = 0.15, inherit.aes = FALSE
  ) +
  # stage time series, all sensors layered on one panel
  geom_line(
    data = stage_hourly,
    aes(x = timestamp, y = stage_avg, colour = site_id),
    linewidth = 0.35, alpha = 0.9, na.rm = TRUE
  ) +
  scale_colour_manual(name = "Sensor", values = sensor_colours, labels = sensor_labels) +
  # vertical lines at primary sensor deployment end dates
  geom_vline(
    data = sensor_end_lines,
    aes(xintercept = date_end),
    linetype = "dashed", colour = "grey20", linewidth = 0.5
  ) +
  geom_text(
    data = sensor_end_lines,
    aes(x = date_end, y = Inf, label = sensor_id),
    angle = 90, hjust = 1.1, vjust = -0.4, size = 3, colour = "grey20"
  ) +
  scale_x_datetime(date_breaks = "1 year", date_labels = "%Y") +
  labs(
    title    = "SSN703 -- Full stage sensor history with rating curve periods",
    subtitle = "Dashed lines = primary sensor deployment end dates | Coloured blocks = rating curve validity window | Red bands = confirmed bad data (untrustworthy, even during nominal overlap)",
    x        = NULL,
    y        = "Stage (m)",
    caption  = paste0(
      "Rating curve period boundaries are authoritative database cutover dates, not physical sensor install dates -- see script header\n",
      "Red bands mark bad_data_start/bad_data_end from sensor_registry -- a sensor can be degraded well before its nominal date_end,\n",
      "so overlap with its replacement is not automatically clean; check red bands before trusting any offset calculated from an overlap window\n",
      "Generated by 02b_plot_full_history_with_ratings.R"
    )
  ) +
  theme_bw() +
  theme(
    legend.position  = "bottom",
    legend.box       = "vertical",
    panel.grid.minor = element_blank(),
    plot.caption     = element_text(hjust = 0, size = 8, colour = "grey40")
  ) +
  guides(colour = guide_legend(override.aes = list(linewidth = 1.5)))


# -----------------------------------------------------------------------------
# 7. Save
# -----------------------------------------------------------------------------

pdf_path <- file.path(plot_dir, "ssn703_sensor_history_overview.pdf")
png_path <- file.path(plot_dir, "ssn703_sensor_history_overview.png")

pdf(pdf_path, width = 14, height = 7)
print(p)
dev.off()

ggsave(png_path, p, width = 14, height = 7, dpi = 200)

message("Saved: ", pdf_path)
message("Saved: ", png_path)
message("\nReminder: rc_periods dates are hardcoded from 10_discharge.R comments --")
message("update them directly in this script if the authoritative cutover dates change.")