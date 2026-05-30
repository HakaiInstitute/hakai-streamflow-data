# =============================================================================
# SSN844 Discharge Summary -- Script 11
# =============================================================================
# Purpose:
#   Produce standard hydrological summary plots and statistics for the
#   SSN844 discharge record (RC2, 2017-07-13 onward).
#
# Catchment area: 5.71 km²
# Discharge units: m³/s at 5-minute resolution
# Specific discharge units: L/s/km²
#
# Inputs:
#   04_outputs/ssn844_discharge_combined.csv
#
# Outputs:
#   04_outputs/summary/ssn844_monthly_volume.png
#   04_outputs/summary/ssn844_annual_volume.png
#   04_outputs/summary/ssn844_specific_discharge.png
#   04_outputs/summary/ssn844_flow_duration_curve.png
#   04_outputs/summary/ssn844_seasonal_distribution.png
#   04_outputs/summary/ssn844_annual_maxima.png
#   04_outputs/summary/ssn844_data_quality.png
#   04_outputs/summary/ssn844_discharge_summary_stats.csv
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)

select <- dplyr::select

# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

catchment_area_km2 <- 5.71
timestep_s         <- 300

out_dir <- "04_outputs/summary"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# -----------------------------------------------------------------------------
# 1. Load discharge
# -----------------------------------------------------------------------------

discharge <- read_csv("04_outputs/ssn844_discharge_combined.csv",
                      show_col_types = FALSE) |>
  mutate(
    timestamp  = as.POSIXct(timestamp, tz = "Etc/GMT+8"),
    year_month = floor_date(timestamp, "month"),
    month      = month(timestamp, label = TRUE, abbr = TRUE),
    Q_spec     = Q_model * 1000 / catchment_area_km2
  )

message("Loaded ", nrow(discharge), " rows")
message("Date range: ", min(discharge$timestamp), " to ", max(discharge$timestamp))
message("NA discharge: ", sum(is.na(discharge$Q_model)), " of ", nrow(discharge),
        " (", round(mean(is.na(discharge$Q_model)) * 100, 1), "%)")

# -----------------------------------------------------------------------------
# 2. Monthly data completeness
# -----------------------------------------------------------------------------

monthly_completeness <- discharge |>
  group_by(year_month, water_year, rating_curve_period) |>
  summarise(
    n_obs      = n(),
    n_na       = sum(is.na(Q_model)),
    n_expected = as.numeric(difftime(
      ceiling_date(first(year_month), "month"),
      floor_date(first(year_month),   "month"),
      units = "secs")) / timestep_s,
    pct_complete = round((n_obs - n_na) / n_expected * 100, 1),
    .groups = "drop"
  ) |>
  mutate(reliable = pct_complete >= 80)

# -----------------------------------------------------------------------------
# 3. Monthly total volume
# -----------------------------------------------------------------------------

monthly_volume <- discharge |>
  filter(!is.na(Q_model)) |>
  group_by(year_month, water_year, rating_curve_period) |>
  summarise(
    volume_m3 = sum(Q_model * timestep_s, na.rm = TRUE),
    .groups   = "drop"
  ) |>
  left_join(monthly_completeness |>
              select(year_month, pct_complete, reliable),
            by = "year_month")

p_monthly <- ggplot(monthly_volume,
                    aes(x = year_month, y = volume_m3 / 1e6, fill = reliable)) +
  geom_col() +
  scale_fill_manual(values = c("TRUE" = "#4A90A4", "FALSE" = "#E07B54"),
                    labels = c("TRUE" = "≥80% complete", "FALSE" = "<80% complete")) +
  scale_x_datetime(date_breaks = "1 year", date_labels = "%Y") +
  theme_bw() +
  xlab(NULL) + ylab("Volume (million m³)") +
  labs(title = "SSN844 -- monthly discharge volume",
       subtitle = "RC2 (Rating 2, loc_1) | 2017-07-13 onward",
       fill = "Data coverage",
       caption = "NA timesteps excluded | Orange = month <80% complete")

ggsave(file.path(out_dir, "ssn844_monthly_volume.png"),
       p_monthly, width = 12, height = 6)
message("Saved: ssn844_monthly_volume.png")

# -----------------------------------------------------------------------------
# 4. Annual total volume
# -----------------------------------------------------------------------------

annual_volume <- discharge |>
  filter(!is.na(Q_model)) |>
  group_by(water_year) |>
  summarise(
    volume_m3    = sum(Q_model * timestep_s, na.rm = TRUE),
    volume_mm    = volume_m3 / (catchment_area_km2 * 1e6) * 1000,
    .groups      = "drop"
  )

p_annual <- ggplot(annual_volume, aes(x = water_year, y = volume_mm)) +
  geom_col(fill = "#4A90A4") +
  geom_text(aes(label = paste0(round(volume_mm), " mm")),
            vjust = -0.5, size = 3) +
  theme_bw() +
  xlab("Water year") + ylab("Annual runoff (mm)") +
  labs(title = "SSN844 -- annual discharge volume",
       caption = paste0("Catchment area: ", catchment_area_km2, " km² | ",
                        "NA timesteps excluded"))

ggsave(file.path(out_dir, "ssn844_annual_volume.png"),
       p_annual, width = 10, height = 6)
message("Saved: ssn844_annual_volume.png")

# -----------------------------------------------------------------------------
# 5. Specific discharge timeseries
# -----------------------------------------------------------------------------

daily_spec <- discharge |>
  filter(!is.na(Q_spec)) |>
  mutate(date = as.Date(timestamp)) |>
  group_by(date, water_year, rating_curve_period) |>
  summarise(Q_spec_mean = mean(Q_spec, na.rm = TRUE), .groups = "drop")

p_spec <- ggplot(daily_spec, aes(x = date, y = Q_spec_mean)) +
  geom_line(linewidth = 0.3, colour = "#333333", alpha = 0.7) +
  theme_bw() +
  xlab(NULL) + ylab("Specific discharge (L/s/km²)") +
  labs(title = "SSN844 -- daily mean specific discharge",
       subtitle = paste0("Catchment area: ", catchment_area_km2, " km²"),
       caption = "RC2 (Rating 2, loc_1)")

ggsave(file.path(out_dir, "ssn844_specific_discharge.png"),
       p_spec, width = 12, height = 6)
message("Saved: ssn844_specific_discharge.png")

# -----------------------------------------------------------------------------
# 6. Flow duration curve
# -----------------------------------------------------------------------------

fdc <- discharge |>
  filter(!is.na(Q_model)) |>
  arrange(desc(Q_model)) |>
  mutate(exceedance_pct = row_number() / n() * 100)

p_fdc <- ggplot(fdc, aes(x = exceedance_pct, y = Q_model)) +
  geom_line(colour = "#4A90A4", linewidth = 0.5) +
  scale_y_log10() +
  theme_bw() +
  xlab("Exceedance probability (%)") + ylab("Discharge (m³/s, log scale)") +
  labs(title = "SSN844 -- flow duration curve",
       subtitle = "RC2 full record (2017-07-13 onward)",
       caption = "NA timesteps excluded")

ggsave(file.path(out_dir, "ssn844_flow_duration_curve.png"),
       p_fdc, width = 10, height = 6)
message("Saved: ssn844_flow_duration_curve.png")

# -----------------------------------------------------------------------------
# 7. Seasonal distribution
# -----------------------------------------------------------------------------

p_seasonal <- discharge |>
  filter(!is.na(Q_model)) |>
  ggplot(aes(x = month, y = Q_model)) +
  geom_boxplot(fill = "#4A90A4", outlier.size = 0.5, outlier.alpha = 0.3) +
  scale_y_log10() +
  theme_bw() +
  xlab("Month") + ylab("Discharge (m³/s, log scale)") +
  labs(title = "SSN844 -- seasonal discharge distribution",
       subtitle = "RC2 full record",
       caption = "5-minute values | NA timesteps excluded")

ggsave(file.path(out_dir, "ssn844_seasonal_distribution.png"),
       p_seasonal, width = 10, height = 6)
message("Saved: ssn844_seasonal_distribution.png")

# -----------------------------------------------------------------------------
# 8. Annual maximum instantaneous discharge
# -----------------------------------------------------------------------------

annual_maxima <- discharge |>
  filter(!is.na(Q_model)) |>
  group_by(water_year) |>
  slice_max(Q_model, n = 1) |>
  select(water_year, timestamp, Q_model, Q_spec, rating_curve_period) |>
  ungroup()

message("\n=== Annual maximum instantaneous discharge ===")
print(annual_maxima)

p_maxima <- ggplot(annual_maxima,
                   aes(x = water_year, y = Q_model)) +
  geom_col(fill = "#4A90A4") +
  geom_text(aes(label = paste0(round(Q_model, 1), " m³/s")),
            vjust = -0.5, size = 3) +
  theme_bw() +
  xlab("Water year") + ylab("Peak discharge (m³/s)") +
  labs(title = "SSN844 -- annual maximum instantaneous discharge",
       caption = "5-minute peak | RC2 (Rating 2) throughout | NA timesteps excluded")

ggsave(file.path(out_dir, "ssn844_annual_maxima.png"),
       p_maxima, width = 10, height = 6)
message("Saved: ssn844_annual_maxima.png")

# -----------------------------------------------------------------------------
# 9. Data quality summary
# -----------------------------------------------------------------------------

quality_summary <- discharge |>
  group_by(water_year, rating_curve_period) |>
  summarise(
    total    = n(),
    n_na     = sum(is.na(Q_model)),
    pct_na   = round(n_na / total * 100, 1),
    pct_raw  = round(sum(discharge_flag == "raw",         na.rm = TRUE) / total * 100, 1),
    pct_gf   = round(sum(grepl("gf", discharge_flag),     na.rm = TRUE) / total * 100, 1),
    .groups  = "drop"
  )

message("\n=== Data quality by water year ===")
print(quality_summary)

p_quality <- quality_summary |>
  pivot_longer(cols = c(pct_raw, pct_gf, pct_na),
               names_to = "flag_type", values_to = "pct") |>
  mutate(flag_type = factor(flag_type,
                             levels = c("pct_raw", "pct_gf", "pct_na"),
                             labels = c("Raw", "Gap-filled", "NA"))) |>
  ggplot(aes(x = water_year, y = pct, fill = flag_type)) +
  geom_col() +
  scale_fill_manual(values = c("Raw"        = "#333333",
                                "Gap-filled" = "#FF7F00",
                                "NA"         = "#E41A1C")) +
  theme_bw() +
  xlab("Water year") + ylab("Percentage of timesteps (%)") +
  labs(title = "SSN844 -- discharge data quality by water year",
       fill = "Data flag",
       caption = "RC2 full record")

ggsave(file.path(out_dir, "ssn844_data_quality.png"),
       p_quality, width = 10, height = 6)
message("Saved: ssn844_data_quality.png")

# -----------------------------------------------------------------------------
# 10. Summary statistics table
# -----------------------------------------------------------------------------

summary_stats <- discharge |>
  filter(!is.na(Q_model)) |>
  group_by(water_year, rating_curve_period) |>
  summarise(
    mean_Q_m3s    = round(mean(Q_model), 3),
    median_Q_m3s  = round(median(Q_model), 3),
    max_Q_m3s     = round(max(Q_model), 3),
    min_Q_m3s     = round(min(Q_model), 3),
    mean_Q_spec   = round(mean(Q_spec), 2),
    annual_vol_mm = round(sum(Q_model * timestep_s) /
                          (catchment_area_km2 * 1e6) * 1000, 1),
    .groups = "drop"
  ) |>
  left_join(quality_summary |> select(water_year, pct_na), by = "water_year")

write_csv(summary_stats, file.path(out_dir, "ssn844_discharge_summary_stats.csv"))
message("Saved: ssn844_discharge_summary_stats.csv")

message("\n=== Summary statistics ===")
print(summary_stats)

message("\nDone. All outputs saved to ", out_dir)
