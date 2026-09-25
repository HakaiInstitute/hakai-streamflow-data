# =============================================================================
# SSN703 Stage QC - Script 02: Visual Inspection of Raw Stage Data
# =============================================================================
# Purpose:
#   Generate a multi-page PDF of plots for visual examination of raw stage
#   data across all SSN703 sensors. This is a human step -- look at the plots,
#   assess what you see, and update metadata (00_build_metadata.R) accordingly
#   before proceeding to offset calculation.
#
# This script does NOT modify data or compute anything. It only plots.
#
# Inputs:
#   02_processing/data_parsed/ssn703_all_raw.rds  -- includes metadata columns from sensor_registry
#   03_docs/metadata/overlap_registry.csv
#
# Outputs:
#   02_processing/plots/ssn703_stage_inspection.pdf
#
# PDF contents (one plot per page):
#   Page 1  -- full record, all sensors, hourly thinned, faceted by sensor
#   Page 2  -- full record, all sensors overlaid on one panel, hourly thinned
#   Page 3  -- overlap: ssn703_a vs ssn703_b (full overlap window)
#   Page 4  -- overlap: ssn703_a vs ssn703_b (install period, first 6 weeks)
#   Page 5  -- overlap: ssn703_b vs ssn703_c (full overlap window)
#   Page 6  -- overlap: ssn703_c vs ssn703_d (full overlap window)
#   Page 7  -- offset (ssn703_a minus ssn703_b) vs stage, coloured by season
#              use this to determine stage_threshold_m in overlap_registry
#
# What to look for:
#   - Spikes, flatlines, drift, or erratic behaviour in individual sensors
#   - Where the failing sensor visibly starts degrading in each overlap
#   - Whether low-flow divergence is stage-dependent or seasonal in a vs b
#   - Whether offset looks consistent across storm peaks in a vs b
#   - Confirm or revise bad_data_start dates in sensor_registry
#   - Page 7 specifically: where does the offset stabilize with stage?
#     That is your stage_threshold_m -- update overlap_registry accordingly
#
# After inspection:
#   - Update 00_build_metadata.R with any revised dates or decisions
#   - Rerun 00_build_metadata.R to regenerate CSVs
#   - Then proceed to 03_offset_calculation.R
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)
library(patchwork)


# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

plot_dir <- "02_processing/plots"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)

# Sensor colours -- consistent across all plots
sensor_colours <- c(
  "ssn703_a"  = "#E41A1C",  # red
  "ssn703_b"  = "#377EB8",  # blue
  "ssn703_c"  = "#4DAF4A",  # green
  "ssn703_d"  = "#984EA3",  # purple
  "ssn703_sa" = "#FF7F00"   # orange
)

sensor_labels <- c(
  "ssn703_a"  = "703a (loc_1, RC1, 2014-2018)",
  "ssn703_b"  = "703b (loc_1, RC1, 2017-2021)",
  "ssn703_c"  = "703c (loc_2, RC2, 2018-2023)",
  "ssn703_d"  = "703d (loc_3, RC3, 2021-ongoing)",
  "ssn703_sa" = "703sa (supplementary, loc_4)"
)

# Season labels and colours for page 7
season_colours <- c(
  "Winter (Dec-Feb)" = "#2166AC",
  "Spring (Mar-May)" = "#4DAF4A",
  "Summer (Jun-Aug)" = "#FF7F00",
  "Fall (Sep-Nov)"   = "#984EA3"
)


# -----------------------------------------------------------------------------
# 1. Load data and metadata
# -----------------------------------------------------------------------------
# sensor_registry metadata (station_id, location_id, sensor_role,
# rating_curve_period, bad_data_start, bad_data_end) is already joined
# to the stage data in 01_load_stage_data.R -- no separate read needed here.

stage <- readRDS("02_processing/data_parsed/ssn703_all_raw.rds")
# RDS contains SSN703 sensors only -- no filter needed

overlap_reg <- read_csv("03_docs/metadata/overlap_registry.csv",
                        show_col_types = FALSE) |>
  filter(station_id == "SSN703")

message("Loaded ", nrow(stage), " rows across ", n_distinct(stage$site_id), " sensors")


# -----------------------------------------------------------------------------
# 2. Thin data for full-record plots
# -----------------------------------------------------------------------------
# Use hourly averages for full-record views only -- reduces rendering time
# without losing the shape of the record. Overlap plots use full 5min data.

stage_hourly <- stage |>
  mutate(timestamp_hour = floor_date(timestamp, "hour")) |>
  group_by(site_id, timestamp_hour) |>
  summarise(stage_avg = mean(stage_avg, na.rm = TRUE), .groups = "drop") |>
  rename(timestamp = timestamp_hour)


# -----------------------------------------------------------------------------
# 3. Helper functions
# -----------------------------------------------------------------------------

# Add overlap window shading to a plot
add_overlap_shading <- function(p, overlap_row, alpha = 0.08) {
  p +
    annotate(
      "rect",
      xmin = overlap_row$overlap_start,
      xmax = overlap_row$overlap_end,
      ymin = -Inf, ymax = Inf,
      fill = "grey50", alpha = alpha
    )
}

# Standard theme for all plots
theme_stage <- function() {
  theme_bw() +
    theme(
      legend.position   = "bottom",
      legend.title      = element_blank(),
      panel.grid.minor  = element_blank(),
      strip.background  = element_rect(fill = "grey95"),
      plot.caption      = element_text(hjust = 0, size = 8, colour = "grey40")
    )
}

# Assign season label from month integer
assign_season <- function(month) {
  case_when(
    month %in% c(12, 1, 2)  ~ "Winter (Dec-Feb)",
    month %in% c(3,  4, 5)  ~ "Spring (Mar-May)",
    month %in% c(6,  7, 8)  ~ "Summer (Jun-Aug)",
    month %in% c(9, 10, 11) ~ "Fall (Sep-Nov)"
  )
}


# -----------------------------------------------------------------------------
# 4. Build plots
# -----------------------------------------------------------------------------

# -- Page 1: Full record, faceted by sensor -----------------------------------

p1 <- stage_hourly |>
  filter(site_id != "ssn703_sa") |>  # supplementary on separate facet
  ggplot(aes(x = timestamp, y = stage_avg, colour = site_id)) +
  geom_line(linewidth = 0.3, na.rm = TRUE) +
  facet_wrap(~ site_id, ncol = 1, labeller = labeller(site_id = sensor_labels)) +
  scale_colour_manual(values = sensor_colours, labels = sensor_labels) +
  scale_x_datetime(date_breaks = "1 year", date_labels = "%Y") +
  labs(
    title   = "SSN703 -- Full stage record by sensor (hourly thinned)",
    x       = NULL,
    y       = "Stage (m)",
    caption = "Grey shading = overlap windows | Red shading = confirmed bad data periods\nPlot generated by 02_inspect_stage.R"
  ) +
  theme_stage() +
  theme(legend.position = "none")

# Add overlap shading
for (i in seq_len(nrow(overlap_reg))) {
  p1 <- add_overlap_shading(p1, overlap_reg[i, ])
}

# Add bad data shading -- pulled from metadata columns joined in 01
bad_data_periods <- stage |>
  distinct(site_id, bad_data_start, bad_data_end) |>
  filter(!is.na(bad_data_start))

for (i in seq_len(nrow(bad_data_periods))) {
  bad_end <- if (is.na(bad_data_periods$bad_data_end[i])) Sys.time() else bad_data_periods$bad_data_end[i]
  p1 <- p1 +
    annotate(
      "rect",
      xmin = bad_data_periods$bad_data_start[i],
      xmax = bad_end,
      ymin = -Inf, ymax = Inf,
      fill = "red", alpha = 0.08
    )
}


# -- Page 2: Full record, all sensors overlaid --------------------------------

p2 <- stage_hourly |>
  ggplot(aes(x = timestamp, y = stage_avg, colour = site_id)) +
  geom_line(linewidth = 0.3, na.rm = TRUE, alpha = 0.8) +
  scale_colour_manual(values = sensor_colours, labels = sensor_labels) +
  scale_x_datetime(date_breaks = "1 year", date_labels = "%Y") +
  labs(
    title   = "SSN703 -- Full stage record, all sensors overlaid (hourly thinned)",
    x       = NULL,
    y       = "Stage (m)",
    caption = "Grey shading = overlap windows\nPlot generated by 02_inspect_stage.R"
  ) +
  theme_stage()

for (i in seq_len(nrow(overlap_reg))) {
  p2 <- add_overlap_shading(p2, overlap_reg[i, ])
}


# -- Page 3: Overlap ssn703_a vs ssn703_b, full overlap window ----------------

ov_ab <- overlap_reg |> filter(sensor_id_failing == "ssn703_a")

stage_ab <- stage |>
  filter(
    site_id %in% c("ssn703_a", "ssn703_b"),
    timestamp >= ov_ab$overlap_start,
    timestamp <= ov_ab$overlap_end
  )

p3 <- stage_ab |>
  ggplot(aes(x = timestamp, y = stage_avg, colour = site_id)) +
  geom_line(linewidth = 0.4, na.rm = TRUE, alpha = 0.85) +
  scale_colour_manual(values = sensor_colours, labels = sensor_labels) +
  scale_x_datetime(date_breaks = "1 month", date_labels = "%b %Y") +
  labs(
    title    = "SSN703 -- Overlap: ssn703_a (failing) vs ssn703_b (replacement)",
    subtitle = "Full overlap window | 5min resolution | offset_method = storm_peaks",
    x        = NULL,
    y        = "Stage (m)",
    caption  = "Look for: consistent offset at storm peaks; seasonal low-flow divergence; when 703a starts degrading\nPlot generated by 02_inspect_stage.R"
  ) +
  theme_stage()

# Add stage threshold line if already set in metadata
if (!is.na(ov_ab$stage_threshold_m)) {
  p3 <- p3 +
    geom_hline(yintercept = ov_ab$stage_threshold_m,
               linetype = "dashed", colour = "grey40", linewidth = 0.5) +
    annotate("text", x = ov_ab$overlap_start, y = ov_ab$stage_threshold_m + 0.02,
             label = paste0("stage threshold = ", ov_ab$stage_threshold_m, " m"),
             hjust = 0, size = 3, colour = "grey40")
}


# -- Page 4: Overlap ssn703_a vs ssn703_b, install period (first 6 weeks) ----

p4 <- stage_ab |>
  filter(timestamp <= ov_ab$overlap_start + weeks(6)) |>
  ggplot(aes(x = timestamp, y = stage_avg, colour = site_id)) +
  geom_line(linewidth = 0.4, na.rm = TRUE, alpha = 0.85) +
  scale_colour_manual(values = sensor_colours, labels = sensor_labels) +
  scale_x_datetime(date_breaks = "1 week", date_labels = "%d %b") +
  labs(
    title    = "SSN703 -- Overlap: ssn703_a vs ssn703_b, install period (first 6 weeks)",
    subtitle = "5min resolution | use to assess install conditions and early offset stability",
    x        = NULL,
    y        = "Stage (m)",
    caption  = "Look for: stable period at install; whether sensors agree immediately; early storm peaks for offset validation\nPlot generated by 02_inspect_stage.R"
  ) +
  theme_stage()


# -- Page 5: Overlap ssn703_b vs ssn703_c, full overlap window ----------------

ov_bc <- overlap_reg |> filter(sensor_id_failing == "ssn703_b")

p5 <- stage |>
  filter(
    site_id %in% c("ssn703_b", "ssn703_c"),
    timestamp >= ov_bc$overlap_start,
    timestamp <= ov_bc$overlap_end
  ) |>
  ggplot(aes(x = timestamp, y = stage_avg, colour = site_id)) +
  geom_line(linewidth = 0.4, na.rm = TRUE, alpha = 0.85) +
  scale_colour_manual(values = sensor_colours, labels = sensor_labels) +
  scale_x_datetime(date_breaks = "3 months", date_labels = "%b %Y") +
  labs(
    title    = "SSN703 -- Overlap: ssn703_b (failing) vs ssn703_c (replacement)",
    subtitle = "Full overlap window | 5min resolution | location break -- no offset computed",
    x        = NULL,
    y        = "Stage (m)",
    caption  = "Look for: confirm sensors are NOT on same water surface; when 703b starts degrading (bad_data_start)\nPlot generated by 02_inspect_stage.R"
  ) +
  theme_stage()


# -- Page 6: Overlap ssn703_c vs ssn703_d, full overlap window ----------------

ov_cd <- overlap_reg |> filter(sensor_id_failing == "ssn703_c")

p6 <- stage |>
  filter(
    site_id %in% c("ssn703_c", "ssn703_d"),
    timestamp >= ov_cd$overlap_start,
    timestamp <= ov_cd$overlap_end
  ) |>
  ggplot(aes(x = timestamp, y = stage_avg, colour = site_id)) +
  geom_line(linewidth = 0.4, na.rm = TRUE, alpha = 0.85) +
  scale_colour_manual(values = sensor_colours, labels = sensor_labels) +
  scale_x_datetime(date_breaks = "3 months", date_labels = "%b %Y") +
  labs(
    title    = "SSN703 -- Overlap: ssn703_c (failing) vs ssn703_d (replacement)",
    subtitle = "Full overlap window | 5min resolution | location break -- no offset computed",
    x        = NULL,
    y        = "Stage (m)",
    caption  = "Look for: confirm sensors are NOT on same water surface; when 703c starts degrading (bad_data_start)\nPlot generated by 02_inspect_stage.R"
  ) +
  theme_stage()


# -- Page 7: Offset (ssn703_a minus ssn703_b) vs stage, coloured by season ---
# Purpose: identify where the offset stabilizes with increasing stage.
# That stage value becomes stage_threshold_m in overlap_registry.
#
# How to read this plot:
#   - x axis: stage on the reference sensor (ssn703_b)
#   - y axis: difference between the two sensors (703a minus 703b)
#   - A true datum offset would show as a horizontal band of points at a
#     consistent y value across all stages and seasons
#   - Scatter at low stage = hydraulic decoupling or sensor artifact -- not reliable
#   - Strong seasonal colour banding = seasonal hydraulic effect, not datum
#   - The stage where points stop scattering and cluster consistently is your threshold

# Pivot to wide so both sensors are columns at matching timestamps
offset_df <- stage_ab |>
  select(timestamp, site_id, stage_avg) |>
  pivot_wider(names_from = site_id, values_from = stage_avg) |>
  filter(!is.na(ssn703_a), !is.na(ssn703_b)) |>
  mutate(
    offset    = ssn703_a - ssn703_b,   # positive = 703a reads higher than 703b
    ref_stage = ssn703_b,              # reference stage from replacement sensor
    month_int = month(timestamp),
    season    = assign_season(month_int)
  )

# Median offset across all points -- reference line only, not the final offset value
median_offset_all <- median(offset_df$offset, na.rm = TRUE)

p7 <- offset_df |>
  ggplot(aes(x = ref_stage, y = offset, colour = season)) +
  geom_point(size = 0.6, alpha = 0.4) +
  geom_hline(yintercept = 0,
             linetype = "solid", colour = "black", linewidth = 0.4) +
  geom_hline(yintercept = median_offset_all,
             linetype = "dashed", colour = "grey30", linewidth = 0.5) +
  annotate("text", x = Inf, y = median_offset_all + 0.003,
           label = paste0("median offset (all points) = ", round(median_offset_all, 4), " m"),
           hjust = 1.05, size = 3, colour = "grey30") +
  scale_colour_manual(values = season_colours) +
  # Add threshold line if already set -- shown as a reminder, not a decision
  {if (!is.na(ov_ab$stage_threshold_m))
    geom_vline(xintercept = ov_ab$stage_threshold_m,
               linetype = "dashed", colour = "grey40", linewidth = 0.5)} +
  {if (!is.na(ov_ab$stage_threshold_m))
    annotate("text", x = ov_ab$stage_threshold_m + 0.01, y = Inf,
             label = paste0("current threshold = ", ov_ab$stage_threshold_m, " m (TBC)"),
             hjust = 0, vjust = 1.5, size = 3, colour = "grey40")} +
  labs(
    title    = "SSN703 -- Offset (ssn703_a minus ssn703_b) vs stage, coloured by season",
    subtitle = "Full overlap window | use to determine stage_threshold_m in overlap_registry",
    x        = "Stage -- ssn703_b reference sensor (m)",
    y        = "Offset: ssn703_a minus ssn703_b (m)",
    caption  = paste0(
      "Dashed horizontal line = median offset across all points (reference only -- not the final offset value)\n",
      "Look for: the stage where points stop scattering and cluster at a consistent offset value\n",
      "Persistent seasonal colour bands at low stage = hydraulic artifact, not datum difference\n",
      "Once threshold is identified: update stage_threshold_m in 00_build_metadata.R, rerun 00, rerun this script\n",
      "Plot generated by 02_inspect_stage.R"
    )
  ) +
  theme_stage()


# -----------------------------------------------------------------------------
# 5. Save to PDF
# -----------------------------------------------------------------------------

out_path <- file.path(plot_dir, "ssn703_stage_inspection.pdf")

pdf(out_path, width = 14, height = 8)
print(p1)
print(p2)
print(p3)
print(p4)
print(p5)
print(p6)
print(p7)
dev.off()

message("Saved: ", out_path)
message("\nNext steps:")
message("  1. Open PDF and examine all pages carefully")
message("  2. Update bad_data_start dates in 00_build_metadata.R where needed")
message("  3. Use page 7 to determine stage_threshold_m for ssn703_a/b overlap")
message("  4. Update stage_threshold_m in 00_build_metadata.R")
message("  5. Rerun 00_build_metadata.R if any changes made, then rerun this script")
message("  6. Proceed to 03_offset_calculation.R")