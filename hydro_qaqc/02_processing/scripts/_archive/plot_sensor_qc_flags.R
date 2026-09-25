# ==============================================================================
# plot_sensor_qc_flags.R
#
# Interactive plotly time series of a single sensor's stage_corrected series,
# colored by qc_flag (raw / gf_sa / gf_spline / gf_spline_event / unfilled).
# Uses scattergl for large point counts (WebGL rendering).
#
# Splits the record by water year (Oct 1 - Sep 30) and saves one interactive
# HTML file per water year, per plot type (points / line), into a per-sensor
# subdirectory -- so individual years can be reviewed/shared without loading
# the full multi-year record.
#
# Run explicitly per sensor file -- set the parameters below and loop
# externally if plotting multiple sensors.
# ==============================================================================

library(readr)
library(dplyr)
library(lubridate)
library(plotly)
library(htmlwidgets)

# ------------------------------------------------------------------------------
# 1. PARAMETERS -- set explicitly per run
# ------------------------------------------------------------------------------

qc_output_path <- "C:/Users/Emily/Documents/git-repos/hydro_qaqc/04_outputs/per_sensor/ssn703_ssn703_c_stage_qc.csv"
sensor_label   <- "PT3 (ssn703_c)"   # used in the plot titles
sensor_short   <- "ssn703_c"         # used in output filenames/folder

# Base directory -- one subfolder per sensor will be created under this
plot_dir <- "C:/Users/Emily/Documents/git-repos/hydro_qaqc/04_outputs/plots_by_year"

# ------------------------------------------------------------------------------
# 2. LOAD DATA
# ------------------------------------------------------------------------------

qc_data <- read_csv(
  qc_output_path,
  col_types = cols(
    timestamp = col_datetime(format = "%Y-%m-%dT%H:%M:%SZ"),
    .default  = col_guess()
  )
)

# Derive water year: Oct 1 - Sep 30, e.g. Oct 2019 - Sep 2020 = WY2020
qc_data <- qc_data |>
  mutate(water_year = if_else(month(timestamp) >= 10, year(timestamp) + 1, year(timestamp)))

# ------------------------------------------------------------------------------
# 3. FIXED COLOR MAPPING -- consistent flag -> color across runs
# ------------------------------------------------------------------------------

flag_colors <- c(
  "raw"             = "#1f77b4",  # blue
  "gf_sa"           = "#ff7f0e",  # orange
  "gf_spline"       = "#2ca02c",  # green
  "gf_spline_event" = "#d62728",  # red
  "unfilled"        = "#7f7f7f"   # grey
)

# ------------------------------------------------------------------------------
# 4. PLOT-BUILDING FUNCTIONS
# ------------------------------------------------------------------------------

build_points_plot <- function(df, sensor_label, wy) {
  plot_ly(
    data = df,
    x = ~timestamp,
    y = ~stage_corrected,
    color = ~qc_flag,
    colors = flag_colors,
    type = "scattergl",
    mode = "markers",
    marker = list(size = 3),
    text = ~paste0("qc_flag: ", qc_flag, "<br>stage: ", round(stage_corrected, 4)),
    hoverinfo = "text+x"
  ) %>%
    layout(
      title = paste0("Stage series (points) by QC flag \u2014 ", sensor_label, " \u2014 WY", wy),
      xaxis = list(title = "Measurement time"),
      yaxis = list(title = "Corrected stage (m)"),
      legend = list(title = list(text = "qc_flag"))
    )
}

build_line_plot <- function(df, sensor_label, wy) {
  
  # Build explicit segments: each point connects to the next point in time,
  # colored by the qc_flag of the segment's starting point. This keeps the
  # line visually continuous across flag transitions (unlike coloring a
  # single trace by a discrete variable, which would break the line at every
  # flag change instead of just changing its color).
  seg_data <- df |>
    arrange(timestamp) |>
    mutate(
      xend = lead(timestamp),
      yend = lead(stage_corrected)
    ) |>
    filter(!is.na(xend))  # drop the last row, which has no next point
  
  p <- plot_ly(colors = flag_colors)
  
  for (flag_val in names(flag_colors)) {
    flag_segs <- seg_data |> filter(qc_flag == flag_val)
    if (nrow(flag_segs) == 0) next
    
    # Build one long line per flag by interleaving each segment's start/end
    # points with NA breaks in between, so plotly draws disjoint segments
    # under a single trace/legend entry rather than one trace per segment
    # (much faster to render for large series).
    x_vals <- as.vector(rbind(flag_segs$timestamp, flag_segs$xend, NA))
    y_vals <- as.vector(rbind(flag_segs$stage_corrected, flag_segs$yend, NA))
    
    p <- p %>% add_trace(
      x = x_vals, y = y_vals,
      type = "scattergl", mode = "lines",
      line = list(color = flag_colors[[flag_val]], width = 2),
      name = flag_val,
      hoverinfo = "x+y"
    )
  }
  
  p %>% layout(
    title = paste0("Stage series (line) by QC flag \u2014 ", sensor_label, " \u2014 WY", wy),
    xaxis = list(title = "Measurement time"),
    yaxis = list(title = "Corrected stage (m)"),
    legend = list(title = list(text = "qc_flag"))
  )
}

# ------------------------------------------------------------------------------
# 5. LOOP OVER WATER YEARS -- build and save both plot types per year
# ------------------------------------------------------------------------------

sensor_dir <- file.path(plot_dir, sensor_short)
dir.create(sensor_dir, showWarnings = FALSE, recursive = TRUE)

water_years <- sort(unique(qc_data$water_year))

for (wy in water_years) {
  
  df_wy <- qc_data |> filter(water_year == wy)
  if (nrow(df_wy) == 0) next
  
  p_points <- build_points_plot(df_wy, sensor_label, wy)
  p_line   <- build_line_plot(df_wy, sensor_label, wy)
  
  points_file <- file.path(sensor_dir, paste0(sensor_short, "_WY", wy, "_points.html"))
  line_file   <- file.path(sensor_dir, paste0(sensor_short, "_WY", wy, "_line.html"))
  
  saveWidget(p_points, points_file, selfcontained = TRUE)
  saveWidget(p_line, line_file, selfcontained = TRUE)
  
  message("Saved: ", points_file)
  message("Saved: ", line_file)
}

message("\nAll per-year plots saved under: ", sensor_dir)