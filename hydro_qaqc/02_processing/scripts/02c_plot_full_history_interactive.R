# =============================================================================
# SSN703 Stage QC - Script 02c: Interactive Sensor History (dygraphs)
# =============================================================================
# Purpose:
#   Interactive version of 02b_plot_full_history_with_ratings.R -- same
#   content (layered sensor stage, RC period shading, bad data bands,
#   sensor-end markers), but zoomable/pannable in the browser without
#   re-rendering the full ~10 years of data on every interaction.
#
# Why dygraphs and not ggplotly():
#   ggplotly() re-serializes every ggplot geom (including all points in
#   every geom_line, plus every geom_rect) into SVG elements in the DOM.
#   At hourly resolution across 5 sensors over ~10 years that's several
#   hundred thousand SVG nodes, which is exactly what bogs down panning
#   and zooming. dygraphs renders to <canvas> instead, and its built-in
#   range selector runs its own internal downsampling for the overview
#   strip, so panning/zooming stays smooth regardless of full-record size.
#   It also has dyShading() and dyEvent() as first-class features, which
#   map directly onto the RC blocks and sensor-end lines from 02b, so this
#   isn't a full rebuild -- just a different set of function calls.
#
# Inputs:
#   02_processing/data_parsed/ssn703_all_raw.rds
#   03_docs/metadata/sensor_registry.csv
#
# Outputs:
#   02_processing/plots/ssn703_sensor_history_interactive.html
#   (self-contained -- opens in any browser, no server needed)
#
# Notes:
#   - Data is thinned to hourly, same as 02b. dygraphs can technically
#     handle full 5-min resolution, but hourly keeps the exported HTML
#     file small and keeps first-load time fast -- there's no analytical
#     reason to go finer for an overview/navigation plot like this one.
#     If you want to inspect a specific window at full resolution, it's
#     better to zoom in here to find the window, then re-run the relevant
#     page of 02_inspect_stage.R (which already plots full 5-min data)
#     for that narrower range.
#   - Same caveat as 02b: RC period boundaries are the authoritative
#     database cutover dates from 10_discharge.R, not physical sensor
#     install dates. Edit `rc_periods` directly if those change.
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)
library(xts)
library(dygraphs)
library(htmlwidgets)

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
# 2. Sensor colours (consistent with 02_inspect_stage.R / 02b)
# -----------------------------------------------------------------------------

sensor_colours <- c(
  ssn703_a  = "#E41A1C",
  ssn703_b  = "#377EB8",
  ssn703_c  = "#4DAF4A",
  ssn703_d  = "#984EA3",
  ssn703_sa = "#FF7F00"
)


# -----------------------------------------------------------------------------
# 3. Rating curve period blocks (authoritative -- see notes in header)
# -----------------------------------------------------------------------------

rc_periods <- tribble(
  ~rc_label,                    ~rc_start,                                          ~rc_end,                                            ~rc_colour,
  "RC1 (loc_1: 703a -> 703b)",  as.POSIXct("2014-08-03 10:55:00", tz = "Etc/GMT+8"), as.POSIXct("2019-02-09 00:00:00", tz = "Etc/GMT+8"), "#B3CDE3",
  "RC2 (loc_2: 703c)",          as.POSIXct("2019-02-09 00:00:00", tz = "Etc/GMT+8"), as.POSIXct("2023-09-15 00:00:00", tz = "Etc/GMT+8"), "#CCEBC5",
  "RC3 (loc_3: 703d)",          as.POSIXct("2023-09-15 00:00:00", tz = "Etc/GMT+8"), now_time,                                            "#DECBE4"
)


# -----------------------------------------------------------------------------
# 4. Sensor deployment-end markers and bad data bands
# -----------------------------------------------------------------------------
# Same distinction as 02b: date_end is when a sensor's record stops, NOT
# when it stopped being trustworthy. bad_data_start/bad_data_end is what
# actually flags the untrustworthy portion, including within an overlap.

sensor_end_lines <- sensor_registry |>
  filter(!is.na(date_end), sensor_role == "primary")

bad_data_periods <- sensor_registry |>
  filter(!is.na(bad_data_start)) |>
  mutate(bad_data_end_plot = if_else(is.na(bad_data_end), now_time, bad_data_end))


# -----------------------------------------------------------------------------
# 5. Thin to hourly and reshape to wide xts
# -----------------------------------------------------------------------------
# dygraphs wants one xts object with one column per series, sharing a
# single time index. Sensors with non-overlapping periods will just carry
# NA outside their own deployment window -- dygraphs leaves gaps at NA by
# default rather than connecting across them.

stage_hourly <- stage |>
  mutate(timestamp_hour = floor_date(timestamp, "hour")) |>
  group_by(site_id, timestamp_hour) |>
  summarise(stage_avg = mean(stage_avg, na.rm = TRUE), .groups = "drop") |>
  rename(timestamp = timestamp_hour)

stage_wide <- stage_hourly |>
  pivot_wider(names_from = site_id, values_from = stage_avg) |>
  arrange(timestamp)

stage_xts <- xts(
  stage_wide |> select(-timestamp),
  order.by = stage_wide$timestamp,
  tzone    = "Etc/GMT+8"
)

# Keep only the sensor columns actually present, in a fixed order so
# dySeries() calls below line up with sensor_colours
sensor_cols <- intersect(names(sensor_colours), colnames(stage_xts))
stage_xts   <- stage_xts[, sensor_cols]


# -----------------------------------------------------------------------------
# 6. Build the dygraph
# -----------------------------------------------------------------------------

dy <- dygraph(
  stage_xts,
  main = "SSN703 -- Full stage sensor history with rating curve periods (interactive)"
) |>
  dyAxis("y", label = "Stage (m)") |>
  dyAxis("x", label = NULL) |>
  dyOptions(
    useDataTimezone     = TRUE,   # keep displayed times in Etc/GMT+8, don't shift to browser locale
    connectSeparatedPoints = FALSE,
    strokeWidth         = 1,
    fillGraph           = FALSE
  ) |>
  dyLegend(show = "always", width = 600) |>
  dyRangeSelector(height = 40)     # the minimap that keeps zoom/pan fast

# Per-sensor colour + display name
for (s in sensor_cols) {
  dy <- dy |> dySeries(s, color = sensor_colours[[s]], label = s)
}

# RC period shading (background bands, drawn behind the data)
for (i in seq_len(nrow(rc_periods))) {
  dy <- dy |> dyShading(
    from  = rc_periods$rc_start[i],
    to    = rc_periods$rc_end[i],
    color = rc_periods$rc_colour[i]
  )
}

# Bad data bands (red, semi-transparent via rgba -- dygraphs accepts CSS colours)
for (i in seq_len(nrow(bad_data_periods))) {
  dy <- dy |> dyShading(
    from  = bad_data_periods$bad_data_start[i],
    to    = bad_data_periods$bad_data_end_plot[i],
    color = "rgba(255, 0, 0, 0.15)"
  )
}

# Sensor deployment-end markers (vertical dashed lines with labels)
for (i in seq_len(nrow(sensor_end_lines))) {
  dy <- dy |> dyEvent(
    x         = sensor_end_lines$date_end[i],
    label     = sensor_end_lines$sensor_id[i],
    labelLoc  = "top",
    color     = "grey20",
    strokePattern = "dashed"
  )
}

# Click-to-toggle legend: clicking a series name in the legend shows/hides
# that series. Vanilla JS, no jQuery dependency. Toggled-off series are
# dimmed in the legend so it's clear at a glance what's currently hidden.
# switched `show = "always"` above (rather than "follow") so the legend
# with clickable series names is visible before you've hovered at all.
dy <- dy |> htmlwidgets::onRender("
  function(el, x) {
    var dygraph = this;
    var legendSpans = el.querySelectorAll('.dygraph-legend > span');
    legendSpans.forEach(function(span, i) {
      span.style.cursor = 'pointer';
      span.addEventListener('click', function() {
        var vis = dygraph.visibility();
        vis[i] = !vis[i];
        dygraph.setVisibility(vis);
        span.style.opacity = vis[i] ? 1 : 0.3;
      });
    });
  }
")


# -----------------------------------------------------------------------------
# 7. Save as self-contained HTML
# -----------------------------------------------------------------------------

out_path <- file.path(plot_dir, "ssn703_sensor_history_interactive.html")

saveWidget(dy, file = normalizePath(out_path, mustWork = FALSE), selfcontained = TRUE)

message("Saved: ", out_path)
message("Open directly in a browser -- drag on the main chart to zoom,")
message("use the range selector strip at the bottom to pan, double-click to reset.")
message("Click any series name in the legend to show/hide that sensor.")