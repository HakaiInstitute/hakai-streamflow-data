# =============================================================================
# SSN703 - Interactive Full Discharge Timeseries (correct boundaries)
# =============================================================================
# Purpose:
#   Computes discharge directly from continuous stage using the correct
#   sensor/rating windows (RC1: station start-2018-09-14, RC2: 2018-09-14
#   to 2023-09-15, RC3: 2023-09-15 onward) and produces an interactive
#   dygraph for efficiently panning/zooming into each transition, rather
#   than relying on ssn703_discharge_combined.csv, which was built with the
#   old (incorrect) 2019-02-09 RC1/RC2 boundary.
#
#   Bad-data windows (ssn703_c 2023-06-25 to 2023-09-14, ssn703_d 2023-10-23
#   to 2023-11-26) are excluded (set to NA/unfilled), not silently
#   interpolated through.
#
#   RC1 segment uses ssn703_b where available, offset-corrected ssn703_a to
#   fill the earlier period -- see caveats below, same as the earlier
#   full-record summary work: RC1 here uses Rating 3 applied across the
#   whole pre-2018-09-14 window as an approximation (Ratings 1/2 sub-period
#   boundaries within that span are not established), so treat this segment
#   as directionally useful for eyeballing transitions, not as a precise
#   reconstruction of the historical published record.
#
# Inputs:
#   02_processing/data_parsed/ssn703_all_raw.rds (or stage_wide, if in environment)
#   04_outputs/ssn703_rating_curve_lookup_combined.csv
#   03_docs/metadata/overlap_registry.csv
#
# Outputs:
#   02_processing/plots/ssn703_full_discharge_interactive.html
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
meta_dir <- "03_docs/metadata"
out_dir  <- "04_outputs"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)

# -----------------------------------------------------------------------------
# 0. Boundaries -- the actual answer to "what dates do I feed it"
# -----------------------------------------------------------------------------

RC1_START <- as.POSIXct("2014-08-03 10:55:00", tz = "Etc/GMT+8")  # station start
RC1_END   <- as.POSIXct("2018-09-14 20:00:00", tz = "Etc/GMT+8")  # = RC2_START
RC2_START <- RC1_END
RC2_END   <- as.POSIXct("2023-09-15 00:00:00", tz = "Etc/GMT+8")  # = RC3_START
RC3_START <- RC2_END

# Known bad-data windows -- excluded from discharge computation, not
# treated as boundary changes
BAD_C_START <- as.POSIXct("2023-06-25 17:00:00", tz = "Etc/GMT+8")
BAD_C_END   <- as.POSIXct("2023-09-14 23:55:00", tz = "Etc/GMT+8")
BAD_D_START <- as.POSIXct("2023-10-23 00:00:00", tz = "Etc/GMT+8")
BAD_D_END   <- as.POSIXct("2023-11-26 23:55:00", tz = "Etc/GMT+8")


# -----------------------------------------------------------------------------
# 1. Load continuous stage
# -----------------------------------------------------------------------------

if (!exists("stage_wide")) {
  stage <- readRDS("02_processing/data_parsed/ssn703_all_raw.rds")
  stage_wide <- stage |>
    mutate(timestamp_hour = floor_date(timestamp, "hour")) |>
    group_by(site_id, timestamp_hour) |>
    summarise(stage_avg = mean(stage_avg, na.rm = TRUE), .groups = "drop") |>
    rename(timestamp = timestamp_hour) |>
    pivot_wider(names_from = site_id, values_from = stage_avg)
}

# ssn703_a offset onto ssn703_b's datum (+0.02m, per overlap_registry)
overlap_registry <- read_csv(file.path(meta_dir, "overlap_registry.csv"), show_col_types = FALSE) |>
  filter(station_id == "SSN703")

a_offset_m <- overlap_registry |>
  filter(sensor_id_failing == "ssn703_a", sensor_id_replacement == "ssn703_b") |>
  pull(notes) |>
  str_extract("[+-]?[0-9.]+(?=m applied)") |>
  as.numeric()

if (length(a_offset_m) == 0 || is.na(a_offset_m)) {
  warning("Could not parse ssn703_a offset -- defaulting to +0.02m. Verify against overlap_registry.")
  a_offset_m <- 0.02
}


# -----------------------------------------------------------------------------
# 2. Load rating curve lookup
# -----------------------------------------------------------------------------

lookup <- read_csv(file.path(out_dir, "ssn703_rating_curve_lookup_combined.csv"), show_col_types = FALSE)

rc1_curve <- lookup |> filter(Rating == 3)  # last v4 curve, applied retrospectively across RC1 window
rc2_curve <- lookup |> filter(Rating == 4)
rc3_curve <- lookup |> filter(Rating == 5)

if (nrow(rc1_curve) == 0 || nrow(rc2_curve) == 0 || nrow(rc3_curve) == 0) {
  stop("One or more expected Ratings (3, 4, 5) not found in the lookup file -- check Rating values")
}


# -----------------------------------------------------------------------------
# 3. Apply rating curve within each window, using the correct sensor
# -----------------------------------------------------------------------------

apply_rating <- function(stage_cm, curve) {
  approx(curve$Stage_avg, curve$Q_model, xout = stage_cm, rule = 1)$y
}

discharge_rc1 <- stage_wide |>
  filter(timestamp >= RC1_START, timestamp < RC1_END) |>
  mutate(stage_m = coalesce(ssn703_b, ssn703_a + a_offset_m)) |>
  filter(!is.na(stage_m)) |>
  transmute(timestamp, Q_RC1 = apply_rating(stage_m * 100, rc1_curve))

discharge_rc2 <- stage_wide |>
  filter(timestamp >= RC2_START, timestamp < RC2_END, !is.na(ssn703_c)) |>
  mutate(
    is_bad = timestamp >= BAD_C_START & timestamp <= BAD_C_END,
    stage_cm = ssn703_c * 100
  ) |>
  transmute(
    timestamp,
    Q_RC2 = if_else(is_bad, NA_real_, apply_rating(stage_cm, rc2_curve))
  )

discharge_rc3 <- stage_wide |>
  filter(timestamp >= RC3_START, !is.na(ssn703_d)) |>
  mutate(
    is_bad = timestamp >= BAD_D_START & timestamp <= BAD_D_END,
    stage_cm = ssn703_d * 100
  ) |>
  transmute(
    timestamp,
    Q_RC3 = if_else(is_bad, NA_real_, apply_rating(stage_cm, rc3_curve))
  )

message("RC1 rows: ", nrow(discharge_rc1), " (", min(discharge_rc1$timestamp), " to ", max(discharge_rc1$timestamp), ")")
message("RC2 rows: ", nrow(discharge_rc2), " (", min(discharge_rc2$timestamp), " to ", max(discharge_rc2$timestamp), ")")
message("RC3 rows: ", nrow(discharge_rc3), " (", min(discharge_rc3$timestamp), " to ", max(discharge_rc3$timestamp), ")")


# -----------------------------------------------------------------------------
# 4. Combine into one xts object (one column per RC period, NA outside its
#    own window -- keeps each period visually distinct and toggleable)
# -----------------------------------------------------------------------------

discharge_full <- discharge_rc1 |>
  full_join(discharge_rc2, by = "timestamp") |>
  full_join(discharge_rc3, by = "timestamp") |>
  arrange(timestamp)

discharge_xts <- xts(
  discharge_full |> select(Q_RC1, Q_RC2, Q_RC3),
  order.by = discharge_full$timestamp,
  tzone    = "Etc/GMT+8"
)


# -----------------------------------------------------------------------------
# 5. Build interactive plot
# -----------------------------------------------------------------------------

dy <- dygraph(
  discharge_xts,
  main = "SSN703 -- Full discharge record, correct sensor/rating boundaries"
) |>
  dyAxis("y", label = "Discharge (m3/s)") |>
  dyAxis("x", label = NULL) |>
  dyOptions(useDataTimezone = TRUE, connectSeparatedPoints = FALSE, strokeWidth = 1) |>
  dySeries("Q_RC1", color = "#888780", label = "RC1 (Rating 3, approx. retrospective)") |>
  dySeries("Q_RC2", color = "#4DAF4A", label = "RC2 (Rating 4, ssn703_c)") |>
  dySeries("Q_RC3", color = "#984EA3", label = "RC3 (Rating 5, ssn703_d)") |>
  dyLegend(show = "always", width = 600) |>
  dyRangeSelector(height = 40) |>
  # RC transition markers -- the two boundaries to actually inspect
  dyEvent(RC1_END, label = "RC1 -> RC2 (2018-09-14)", labelLoc = "top",
          color = "#4DAF4A", strokePattern = "dashed") |>
  dyEvent(RC2_END, label = "RC2 -> RC3 (2023-09-15)", labelLoc = "top",
          color = "#984EA3", strokePattern = "dashed") |>
  # Bad-data windows -- shaded so they're not mistaken for real transition behaviour
  dyShading(from = BAD_C_START, to = BAD_C_END, color = "rgba(255, 0, 0, 0.15)") |>
  dyShading(from = BAD_D_START, to = BAD_D_END, color = "rgba(255, 0, 0, 0.15)")

# Click-to-toggle legend, same pattern as earlier interactive plots
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
# 6. Save
# -----------------------------------------------------------------------------

out_path <- file.path(plot_dir, "ssn703_full_discharge_interactive.html")
saveWidget(dy, file = normalizePath(out_path, mustWork = FALSE), selfcontained = TRUE)

message("\nSaved: ", out_path)
message("Drag on the main chart to zoom into a transition; use the range selector")
message("strip at the bottom to navigate the full ~10-year record without lag.")
message("Click a series name in the legend to isolate one RC period at a time --")
message("useful for looking at just the few weeks on either side of a boundary.")
message("Red shading = known bad-data windows, excluded from discharge (not a")
message("real transition feature).")
message("\nRC1 (grey) is an approximation -- see script header before treating")
message("its values as the real historical record.")