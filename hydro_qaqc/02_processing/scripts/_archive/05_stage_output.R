# =============================================================================
# SSN703 Stage QC - Script 05: Final Stage Output
# =============================================================================
# Purpose:
#   Produce clean, analysis-ready stage output files from the QC'd record.
#   Two output structures:
#
#   1. Per rating curve period -- primary output for rating curve work
#      RC1: ssn703_a (corrected) + ssn703_b, chronological, no overlap
#           703a is used up to overlap_start; 703b is authoritative from overlap_start
#      RC2: ssn703_c, full deployment
#      RC3: ssn703_d, full deployment
#
#   2. Per sensor -- reference output, one file per sensor
#
# Inputs:
#   02_processing/data_parsed/ssn703_qc.rds
#   03_docs/metadata/overlap_registry.csv
#
# Outputs:
#   02_processing/data_output/per_rc/ssn703_RC1_stage_qc.csv
#   02_processing/data_output/per_rc/ssn703_RC2_stage_qc.csv
#   02_processing/data_output/per_rc/ssn703_RC3_stage_qc.csv
#   02_processing/data_output/per_sensor/ssn703_a_stage_qc.csv
#   02_processing/data_output/per_sensor/ssn703_b_stage_qc.csv
#   02_processing/data_output/per_sensor/ssn703_c_stage_qc.csv
#   02_processing/data_output/per_sensor/ssn703_d_stage_qc.csv
#   02_processing/plots/ssn703_stage_final.pdf
#
# Output columns:
#   station_id, site_id, location_id, rating_curve_period
#   timestamp, water_year
#   stage_corrected, offset_applied, qc_flag
#
# Notes:
#   - Supplementary sensor (ssn703_sa) is not included in output files --
#     it was used for gap filling only
#   - Per-RC files are chronological with no overlap period -- ssn703_b is
#     authoritative from its install date onward for RC1
#   - Bad data periods are retained in output with qc_flag = "bad_data" for
#     full traceability -- filter on qc_flag downstream as needed
#   - offset_applied column shows what correction was applied to each row
#     (0 for all sensors except ssn703_a where it is +0.02m)
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(patchwork)
library(lubridate)


# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

data_dir   <- "02_processing/data_parsed"
out_rc_dir <- "02_processing/data_output/per_rc"
out_sn_dir <- "02_processing/data_output/per_sensor"
plot_dir   <- "02_processing/plots"

dir.create(out_rc_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(out_sn_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(plot_dir,   showWarnings = FALSE, recursive = TRUE)

# Output columns -- consistent across all files
output_cols <- c(
  "station_id", "site_id", "location_id", "rating_curve_period",
  "timestamp", "water_year",
  "stage_corrected", "offset_applied", "qc_flag"
)

# Sensor colours for plots
sensor_colours <- c(
  "ssn703_a" = "#E41A1C",
  "ssn703_b" = "#377EB8",
  "ssn703_c" = "#4DAF4A",
  "ssn703_d" = "#984EA3"
)

flag_colours <- c(
  "raw"              = "grey60",
  "bad_data"         = "#E41A1C",
  "spike"            = "#FF7F00",
  "gf_sa"            = "#4DAF4A",
  "gf_spline"        = "#377EB8",
  "gf_spline_event"  = "#984EA3",
  "unfilled"         = "black"
)


# -----------------------------------------------------------------------------
# 1. Load data
# -----------------------------------------------------------------------------

stage <- readRDS(file.path(data_dir, "ssn703_qc.rds")) |>
  filter(sensor_role == "primary")  # exclude ssn703_sa

overlap_reg <- read_csv("03_docs/metadata/overlap_registry.csv",
                        show_col_types = FALSE) |>
  filter(station_id == "SSN703")

message("Loaded ", nrow(stage), " rows across ", n_distinct(stage$site_id), " sensors")


# -----------------------------------------------------------------------------
# 2. Per-sensor output
# -----------------------------------------------------------------------------
# One file per sensor, full deployment window, all QC flags retained.
# Straightforward -- no overlap handling needed here.

message("\n--- Writing per-sensor files ---")

stage |>
  select(all_of(output_cols)) |>
  group_by(site_id) |>
  group_walk(~ {
    out_path <- file.path(out_sn_dir, paste0("ssn703_", .y$site_id, "_stage_qc.csv"))
    write_csv(.x |> mutate(site_id = .y$site_id), out_path)
    message("Saved: ", out_path, " (", nrow(.x), " rows)")
  })


# -----------------------------------------------------------------------------
# 3. Per-RC output
# -----------------------------------------------------------------------------
# One file per rating curve period. RC1 requires overlap handling -- ssn703_b
# is authoritative from its install date onward, so the overlap period uses
# ssn703_b only. ssn703_a contributes data up to ssn703_b install date.

message("\n--- Writing per-RC files ---")

# -- RC1: ssn703_a up to ssn703_b install, then ssn703_b through to its end --

# Get the overlap start for a->b (= ssn703_b install date)
ov_ab_start <- overlap_reg |>
  filter(sensor_id_failing == "ssn703_a") |>
  pull(overlap_start)

rc1 <- bind_rows(
  # ssn703_a: up to but not including the overlap start
  stage |>
    filter(site_id == "ssn703_a", timestamp < ov_ab_start),
  # ssn703_b: from install date onward (authoritative through overlap and beyond)
  stage |>
    filter(site_id == "ssn703_b")
) |>
  arrange(timestamp) |>
  mutate(rating_curve_period = "RC1") |>
  select(all_of(output_cols))

# Sanity check -- no duplicate timestamps
rc1_dups <- rc1 |>
  group_by(timestamp) |>
  filter(n() > 1)

if (nrow(rc1_dups) > 0) {
  warning("RC1: duplicate timestamps found at overlap boundary -- review ov_ab_start")
  print(rc1_dups)
} else {
  message("RC1: no duplicate timestamps")
}

write_csv(rc1, file.path(out_rc_dir, "ssn703_RC1_stage_qc.csv"))
message("Saved: ssn703_RC1_stage_qc.csv (", nrow(rc1), " rows, ",
        n_distinct(rc1$site_id), " sensors)")


# -- RC2: ssn703_c, full deployment --

rc2 <- stage |>
  filter(site_id == "ssn703_c") |>
  arrange(timestamp) |>
  select(all_of(output_cols))

write_csv(rc2, file.path(out_rc_dir, "ssn703_RC2_stage_qc.csv"))
message("Saved: ssn703_RC2_stage_qc.csv (", nrow(rc2), " rows)")


# -- RC3: ssn703_d, full deployment --

rc3 <- stage |>
  filter(site_id == "ssn703_d") |>
  arrange(timestamp) |>
  select(all_of(output_cols))

write_csv(rc3, file.path(out_rc_dir, "ssn703_RC3_stage_qc.csv"))
message("Saved: ssn703_RC3_stage_qc.csv (", nrow(rc3), " rows)")


# -----------------------------------------------------------------------------
# 4. Final summary plot
# -----------------------------------------------------------------------------
# One page per RC period showing the clean stitched record coloured by:
#   - sensor (to see transitions clearly)
#   - qc_flag (to see what was filled/flagged)

plot_rc <- function(df, rc_label) {
  
  # Panel A: coloured by sensor
  p_sensor <- df |>
    ggplot(aes(x = timestamp, y = stage_corrected, colour = site_id)) +
    geom_point(size = 0.3, na.rm = TRUE, alpha = 0.6) +
    scale_colour_manual(values = sensor_colours) +
    scale_x_datetime(date_breaks = "6 months", date_labels = "%b %Y") +
    labs(
      title    = paste0("SSN703 -- ", rc_label, " final stage record"),
      subtitle = "Top: coloured by sensor | Bottom: coloured by QC flag",
      x        = NULL,
      y        = "Stage corrected (m)",
      colour   = "Sensor"
    ) +
    theme_bw() +
    theme(
      legend.position  = "bottom",
      panel.grid.minor = element_blank()
    )
  
  # Panel B: coloured by QC flag
  p_flag <- df |>
    ggplot(aes(x = timestamp, y = stage_corrected, colour = qc_flag)) +
    geom_point(size = 0.3, na.rm = TRUE, alpha = 0.6) +
    scale_colour_manual(values = flag_colours, drop = FALSE) +
    scale_x_datetime(date_breaks = "6 months", date_labels = "%b %Y") +
    labs(
      x       = NULL,
      y       = "Stage corrected (m)",
      colour  = "QC flag",
      caption = "Plot generated by 05_stage_output.R"
    ) +
    theme_bw() +
    theme(
      legend.position  = "bottom",
      panel.grid.minor = element_blank(),
      plot.caption     = element_text(hjust = 0, size = 8, colour = "grey40")
    )
  
  p_sensor / p_flag
}

out_path <- file.path(plot_dir, "ssn703_stage_final.pdf")
pdf(out_path, width = 14, height = 10)
print(plot_rc(rc1, "RC1"))
print(plot_rc(rc2, "RC2"))
print(plot_rc(rc3, "RC3"))
dev.off()

message("\nSaved: ", out_path)


# -----------------------------------------------------------------------------
# 5. Output summary
# -----------------------------------------------------------------------------

message("\n--- Output summary ---")
message("Per-RC files:")
message("  RC1: ", nrow(rc1), " rows | sensors: ",
        paste(unique(rc1$site_id), collapse = ", "))
message("  RC2: ", nrow(rc2), " rows | sensor: ssn703_c")
message("  RC3: ", nrow(rc3), " rows | sensor: ssn703_d")

message("\nQC flag breakdown per RC:")
bind_rows(rc1, rc2, rc3) |>
  count(rating_curve_period, qc_flag) |>
  pivot_wider(names_from = qc_flag, values_from = n, values_fill = 0) |>
  print()

message("\nDone -- stage QC complete for SSN703")
message("Next step: proceed to rating curve development")
message("  RC1 input: ", file.path(out_rc_dir, "ssn703_RC1_stage_qc.csv"))
message("  RC2 input: ", file.path(out_rc_dir, "ssn703_RC2_stage_qc.csv"))
message("  RC3 input: ", file.path(out_rc_dir, "ssn703_RC3_stage_qc.csv"))