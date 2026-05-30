# =============================================================================
# SSN703 Stage QC - Script 03: Datum Offset Calculation and Application
# =============================================================================
# Purpose:
#   Formally record, apply, and validate the datum offset between ssn703_a
#   and ssn703_b. Produces offsets.csv as the documented offset record, and
#   a validation plot confirming the correction looks reasonable.
#
# Background:
#   ssn703_a and ssn703_b overlap from 2017-11-13 to 2018-09-12 at the same
#   physical location (loc_1). Visual inspection of the offset vs stage plot
#   (page 7, 02_inspect_stage.R) showed:
#     - Severe hydraulic decoupling below ~0.8m -- offset not reliable
#     - Seasonal structure persists even at high flows
#     - High-flow offset clusters consistently between -0.01 and -0.02m
#   Decision: offset = -0.02m (ssn703_a reads ~0.02m lower than ssn703_b)
#   Uncertainty: +/- 0.01m based on seasonal spread at high flows
#   Method: expert judgement from offset vs stage plot; no formal survey
#
#   ssn703_b -> ssn703_c and ssn703_c -> ssn703_d are location breaks.
#   No offsets are computed or applied for those transitions -- they are
#   handled as separate rating curve periods (RC2, RC3).
#
# Inputs:
#   02_processing/data_parsed/ssn703_all_raw.rds
#   03_docs/metadata/overlap_registry.csv
#
# Outputs:
#   03_docs/metadata/offsets.csv          -- formal offset record
#   02_processing/data_parsed/ssn703_corrected.rds  -- stage record with offset applied
#   02_processing/plots/ssn703_offset_validation.pdf -- validation plot
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

meta_dir <- "03_docs/metadata"
data_dir <- "02_processing/data_parsed"


# -----------------------------------------------------------------------------
# 1. Define offsets formally
# -----------------------------------------------------------------------------
# This is the authoritative offset record for SSN703.
# Edit this table if offset decisions change -- do not hardcode values elsewhere.
#
# Columns:
#   station_id           -- station identifier
#   sensor_id_failing    -- sensor the offset is applied TO (the older sensor)
#   sensor_id_reference  -- sensor used as datum reference (the replacement)
#   offset_m             -- value added to sensor_id_failing stage to bring it
#                          onto the reference datum (m)
#                          negative = failing sensor reads higher than reference
#                          positive = failing sensor reads lower than reference
#   offset_uncertainty_m -- estimated uncertainty (m); based on seasonal spread
#                          at high flows in offset vs stage plot
#   offset_method        -- how offset was determined
#   offset_basis         -- stage range or conditions used
#   notes                -- reasoning and caveats

offsets <- tribble(
  ~station_id, ~sensor_id_failing, ~sensor_id_reference, ~offset_m, ~offset_uncertainty_m, ~offset_method,    ~offset_basis,                                         ~notes,
  "SSN703",    "ssn703_a",         "ssn703_b",           0.02,      0.01,                  "expert_judgement", "high flow conditions (stage > ~0.8m on ssn703_b)",   "ssn703_a reads consistently lower than ssn703_b; +0.02m added to 703a to bring onto 703b datum; seasonal spread persists at high flows; no formal survey; uncertainty +/-0.01m based on seasonal spread"
)

# Save offsets.csv
write_csv(offsets, file.path(meta_dir, "offsets.csv"))
message("Saved: ", file.path(meta_dir, "offsets.csv"))


# -----------------------------------------------------------------------------
# 2. Load stage data
# -----------------------------------------------------------------------------

stage <- readRDS(file.path(data_dir, "ssn703_all_raw.rds"))

message("Loaded ", nrow(stage), " rows across ", n_distinct(stage$site_id), " sensors")


# -----------------------------------------------------------------------------
# 3. Apply offset to ssn703_a
# -----------------------------------------------------------------------------
# The offset corrects ssn703_a onto the ssn703_b datum.
# All other sensors are unchanged -- ssn703_b is the reference for RC1,
# ssn703_c and ssn703_d start new rating curve periods with no correction.
#
# A new column stage_corrected is added:
#   - For ssn703_a: stage_avg + offset_m
#   - For all other sensors: stage_avg unchanged
#
# The original stage_avg is preserved for traceability.

offset_a <- offsets |>
  filter(sensor_id_failing == "ssn703_a") |>
  pull(offset_m)

stage_corrected <- stage |>
  mutate(
    stage_corrected = case_when(
      site_id == "ssn703_a" ~ stage_avg + offset_a,
      TRUE                  ~ stage_avg
    ),
    offset_applied = case_when(
      site_id == "ssn703_a" ~ offset_a,
      TRUE                  ~ 0
    )
  )

# Quick check -- show mean difference before and after for ssn703_a
message("\n--- Offset application check ---")
stage_corrected |>
  filter(site_id == "ssn703_a") |>
  summarise(
    mean_raw       = mean(stage_avg,       na.rm = TRUE),
    mean_corrected = mean(stage_corrected, na.rm = TRUE),
    difference     = mean_corrected - mean_raw
  ) |>
  print()


# -----------------------------------------------------------------------------
# 4. Validation plot
# -----------------------------------------------------------------------------
# Two-panel plot showing the transition period between ssn703_a and ssn703_b:
#   Top panel:    raw stage -- shows the uncorrected offset between sensors
#   Bottom panel: corrected stage -- ssn703_a shifted onto ssn703_b datum
#
# What to look for:
#   - Bottom panel should show a smooth, continuous transition at the
#     sensor changeover -- no sudden jump
#   - If a jump remains, the offset value needs revisiting

overlap_reg <- read_csv(file.path(meta_dir, "overlap_registry.csv"),
                        show_col_types = FALSE) |>
  filter(sensor_id_failing == "ssn703_a")

# Focus on overlap window only for validation
validation_data <- stage_corrected |>
  filter(
    site_id %in% c("ssn703_a", "ssn703_b"),
    timestamp >= overlap_reg$overlap_start,
    timestamp <= overlap_reg$overlap_end
  )

sensor_colours <- c(
  "ssn703_a" = "#E41A1C",
  "ssn703_b" = "#377EB8"
)

sensor_labels <- c(
  "ssn703_a" = "703a (failing, offset applied)",
  "ssn703_b" = "703b (reference)"
)

# Top panel: raw
p_raw <- validation_data |>
  ggplot(aes(x = timestamp, y = stage_avg, colour = site_id)) +
  geom_line(linewidth = 0.4, na.rm = TRUE, alpha = 0.85) +
  scale_colour_manual(values = sensor_colours, labels = sensor_labels) +
  scale_x_datetime(date_breaks = "1 month", date_labels = "%b %Y") +
  labs(
    title   = "SSN703 -- Offset validation: raw vs corrected stage",
    subtitle = "Top: raw stage | Bottom: corrected stage (ssn703_a + offset)",
    x       = NULL,
    y       = "Stage raw (m)"
  ) +
  theme_bw() +
  theme(
    legend.position  = "none",
    panel.grid.minor = element_blank(),
    plot.subtitle    = element_text(size = 9, colour = "grey40")
  )

# Bottom panel: corrected
p_corrected <- validation_data |>
  ggplot(aes(x = timestamp, y = stage_corrected, colour = site_id)) +
  geom_line(linewidth = 0.4, na.rm = TRUE, alpha = 0.85) +
  scale_colour_manual(values = sensor_colours, labels = sensor_labels) +
  scale_x_datetime(date_breaks = "1 month", date_labels = "%b %Y") +
  annotate("text", x = overlap_reg$overlap_start, y = Inf,
           label = paste0("offset applied to 703a: ", offset_a, " m"),
           hjust = 0, vjust = 1.5, size = 3, colour = "grey40") +
  labs(
    x       = NULL,
    y       = "Stage corrected (m)",
    caption = paste0(
      "Offset: ", offset_a, " m | Uncertainty: +/-0.01 m | Method: expert judgement from offset vs stage plot\n",
      "Look for: smooth transition between sensors -- a remaining jump means offset needs revisiting\n",
      "Plot generated by 03_offset_calculation.R"
    )
  ) +
  theme_bw() +
  theme(
    legend.position  = "bottom",
    legend.title     = element_blank(),
    panel.grid.minor = element_blank(),
    plot.caption     = element_text(hjust = 0, size = 8, colour = "grey40")
  )

# Save validation PDF
out_path <- file.path(plot_dir, "ssn703_offset_validation.pdf")
pdf(out_path, width = 14, height = 10)
print(p_raw / p_corrected)  # patchwork stacking
dev.off()

message("Saved: ", out_path)


# -----------------------------------------------------------------------------
# 5. Save corrected stage record
# -----------------------------------------------------------------------------

saveRDS(stage_corrected, file.path(data_dir, "ssn703_corrected.rds"))

message("Saved: ", file.path(data_dir, "ssn703_corrected.rds"))
message("\nNext steps:")
message("  1. Open ssn703_offset_validation.pdf and check the bottom panel")
message("  2. If transition looks smooth -- proceed to 04_stage_qc.R")
message("  3. If a jump remains -- revisit offset_m in this script and rerun")