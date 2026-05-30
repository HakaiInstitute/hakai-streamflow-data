# =============================================================================
# SSN703 Discharge Summary -- Script 11
# =============================================================================
# Purpose:
#   Produce standard hydrological summary plots and statistics for the
#   SSN703 discharge record (RC2 + RC3, 2019-02-09 onward).
#
# Catchment area: 12.79 km²
# Discharge units: m³/s at 5-minute resolution
# Specific discharge units: L/s/km²
#
# Inputs:
#   04_outputs/ssn703_discharge_combined.csv
#
# Outputs:
#   04_outputs/summary/ssn703_monthly_volume.png
#   04_outputs/summary/ssn703_annual_volume.png
#   04_outputs/summary/ssn703_specific_discharge.png
#   04_outputs/summary/ssn703_flow_duration_curve.png
#   04_outputs/summary/ssn703_seasonal_distribution.png
#   04_outputs/summary/ssn703_annual_maxima.png
#   04_outputs/summary/ssn703_data_quality.png
#   04_outputs/summary/ssn703_discharge_summary_stats.csv
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)

# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

catchment_area_km2 <- 12.79
timestep_s         <- 300   # 5-minute data

out_dir <- "04_outputs/summary"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

rc_transition <- as.POSIXct("2023-09-15", tz = "UTC")

# -----------------------------------------------------------------------------
# 1. Load discharge
# -----------------------------------------------------------------------------

discharge <- read_csv("04_outputs/ssn703_discharge_combined.csv",
                      show_col_types = FALSE) |>
  mutate(
    timestamp  = as.POSIXct(timestamp, tz = "UTC"),
    year_month = floor_date(timestamp, unit = "month"),
    month      = month(timestamp, label = TRUE, abbr = TRUE),
    # specific discharge: m3/s -> L/s/km2
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
  group_by(year_month, water_year, rating_label) |>
  summarise(
    n_obs      = n(),
    n_na       = sum(is.na(Q_model)),
    n_expected = as.numeric(difftime(
      ceiling_date(first(year_month), "month"),
      floor_date(first(year_month), "month"),
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
  group_by(year_month, water_year, rating_label) |>
  summarise(
    volume_m3 = sum(Q_model * timestep_s, na.rm = TRUE),
    .groups   = "drop"
  ) |>
  left_join(monthly_completeness |>
              dplyr::select(year_month, pct_complete, reliable),
            by = "year_month")

p_monthly <- ggplot(monthly_volume,
                    aes(x = year_month, y = volume_m3 / 1e6, fill = reliable)) +
  geom_col() +
  geom_vline(xintercept = rc_transition, linetype = "dashed", colour = "grey40") +
  annotate("text", x = rc_transition, y = Inf,
           label = "RC3\nstart", vjust = 1.5, hjust = -0.1,
           size = 3, colour = "grey40") +
  scale_fill_manual(values = c("TRUE" = "#4A90A4", "FALSE" = "#E07B54"),
                    labels = c("TRUE" = "≥80% complete", "FALSE" = "<80% complete")) +
  theme_bw() +
  xlab(NULL) + ylab("Volume (million m³)") +
  labs(title = "SSN703 -- monthly discharge volume",
       subtitle = "RC2 (loc_2) left of dashed line | RC3 (loc_3) right",
       fill = "Data coverage",
       caption = "NA timesteps excluded | Orange = month <80% complete")

ggsave(file.path(out_dir, "ssn703_monthly_volume.png"),
       p_monthly, width = 12, height = 6)
message("Saved: ssn703_monthly_volume.png")


# -----------------------------------------------------------------------------
# 4. Annual total volume
# -----------------------------------------------------------------------------

annual_volume <- discharge |>
  filter(!is.na(Q_model)) |>
  group_by(water_year) |>
  summarise(
    volume_m3    = sum(Q_model * timestep_s, na.rm = TRUE),
    volume_mm    = volume_m3 / (catchment_area_km2 * 1e6) * 1000,
    n_obs        = n(),
    n_na         = sum(is.na(Q_model)),
    pct_complete = round((n_obs) / (n_obs + n_na) * 100, 1),
    .groups      = "drop"
  )

p_annual <- ggplot(annual_volume,
                   aes(x = water_year, y = volume_mm)) +
  geom_col(fill = "#4A90A4") +
  geom_text(aes(label = paste0(round(volume_mm), " mm")),
            vjust = -0.5, size = 3) +
  theme_bw() +
  xlab("Water year") + ylab("Annual runoff (mm)") +
  labs(title = "SSN703 -- annual discharge volume",
       caption = paste0("Catchment area: ", catchment_area_km2, " km² | ",
                        "NA timesteps excluded"))

ggsave(file.path(out_dir, "ssn703_annual_volume.png"),
       p_annual, width = 10, height = 6)
message("Saved: ssn703_annual_volume.png")


# -----------------------------------------------------------------------------
# 5. Specific discharge timeseries
# -----------------------------------------------------------------------------

# Downsample to daily mean for plotting clarity
daily_spec <- discharge |>
  filter(!is.na(Q_spec)) |>
  mutate(date = as.Date(timestamp)) |>
  group_by(date, water_year, rating_label) |>
  summarise(Q_spec_mean = mean(Q_spec, na.rm = TRUE), .groups = "drop")

p_spec <- ggplot(daily_spec, aes(x = date, y = Q_spec_mean)) +
  geom_line(linewidth = 0.3, colour = "#333333", alpha = 0.7) +
  geom_vline(xintercept = as.Date(rc_transition), linetype = "dashed",
             colour = "grey40") +
  theme_bw() +
  xlab(NULL) + ylab("Specific discharge (L/s/km²)") +
  labs(title = "SSN703 -- daily mean specific discharge",
       subtitle = paste0("Catchment area: ", catchment_area_km2, " km²"),
       caption = "RC2 (loc_2) left of dashed line | RC3 (loc_3) right")

ggsave(file.path(out_dir, "ssn703_specific_discharge.png"),
       p_spec, width = 12, height = 6)
message("Saved: ssn703_specific_discharge.png")


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
  labs(title = "SSN703 -- flow duration curve",
       subtitle = "Full record: RC2 + RC3 combined (2019-02-09 onward)",
       caption = "NA timesteps excluded")

ggsave(file.path(out_dir, "ssn703_flow_duration_curve.png"),
       p_fdc, width = 10, height = 6)
message("Saved: ssn703_flow_duration_curve.png")


# -----------------------------------------------------------------------------
# 7. Seasonal distribution (monthly boxplots)
# -----------------------------------------------------------------------------

p_seasonal <- discharge |>
  filter(!is.na(Q_model)) |>
  ggplot(aes(x = month, y = Q_model)) +
  geom_boxplot(fill = "#4A90A4", outlier.size = 0.5, outlier.alpha = 0.3) +
  scale_y_log10() +
  theme_bw() +
  xlab("Month") + ylab("Discharge (m³/s, log scale)") +
  labs(title = "SSN703 -- seasonal discharge distribution",
       subtitle = "Full record: RC2 + RC3 combined",
       caption = "5-minute values | NA timesteps excluded")

ggsave(file.path(out_dir, "ssn703_seasonal_distribution.png"),
       p_seasonal, width = 10, height = 6)
message("Saved: ssn703_seasonal_distribution.png")


# -----------------------------------------------------------------------------
# 8. Annual maximum instantaneous discharge
# -----------------------------------------------------------------------------

annual_maxima <- discharge |>
  filter(!is.na(Q_model)) |>
  group_by(water_year) |>
  slice_max(Q_model, n = 1) |>
  dplyr::select(water_year, timestamp, Q_model, Q_spec, rating_label) |>
  ungroup()

message("=== Annual maximum instantaneous discharge ===")
print(annual_maxima)

p_maxima <- ggplot(annual_maxima, aes(x = water_year, y = Q_model)) +
  geom_col(fill = "#4A90A4") +
  geom_text(aes(label = paste0(round(Q_model, 1), " m³/s")),
            vjust = -0.5, size = 3) +
  theme_bw() +
  xlab("Water year") + ylab("Peak discharge (m³/s)") +
  labs(title = "SSN703 -- annual maximum instantaneous discharge",
       caption = paste0("5-minute peak | NA timesteps excluded\n",
                        "RC2 (Rating 4, ssn703_c, loc_2): 2019-02-09 to 2023-06-25 | ",
                        "RC3 (Rating 5, ssn703_d, loc_3): 2023-09-15 onward\n",
                        "Note: ssn703_d installed 2021-09-02 -- RC3 stage source present from WY2021-2022 onward"))

ggsave(file.path(out_dir, "ssn703_annual_maxima.png"),
       p_maxima, width = 10, height = 6)
message("Saved: ssn703_annual_maxima.png")


# -----------------------------------------------------------------------------
# 9. Data quality summary
# -----------------------------------------------------------------------------

quality_summary <- discharge |>
  group_by(water_year, rating_label) |>
  summarise(
    total      = n(),
    n_na       = sum(is.na(Q_model)),
    pct_na     = round(n_na / total * 100, 1),
    pct_raw    = round(sum(discharge_flag == "raw", na.rm = TRUE) / total * 100, 1),
    pct_gf     = round(sum(grepl("gf", discharge_flag), na.rm = TRUE) / total * 100, 1),
    .groups    = "drop"
  )

message("=== Data quality by water year ===")
print(quality_summary)

p_quality <- quality_summary |>
  pivot_longer(cols = c(pct_raw, pct_gf, pct_na),
               names_to = "flag_type", values_to = "pct") |>
  mutate(flag_type = factor(flag_type,
                            levels = c("pct_raw", "pct_gf", "pct_na"),
                            labels = c("Raw", "Gap-filled", "NA"))) |>
  ggplot(aes(x = water_year, y = pct, fill = flag_type)) +
  geom_col() +
  scale_fill_manual(values = c("Raw" = "#333333",
                                "Gap-filled" = "#FF7F00",
                                "NA" = "#E41A1C")) +
  theme_bw() +
  xlab("Water year") + ylab("Percentage of timesteps (%)") +
  labs(title = "SSN703 -- discharge data quality by water year",
       fill = "Data flag",
       caption = "RC2 + RC3 combined")

ggsave(file.path(out_dir, "ssn703_data_quality.png"),
       p_quality, width = 10, height = 6)
message("Saved: ssn703_data_quality.png")


# -----------------------------------------------------------------------------
# 10. Summary statistics table
# -----------------------------------------------------------------------------

summary_stats <- discharge |>
  filter(!is.na(Q_model)) |>
  group_by(water_year, rating_label) |>
  summarise(
    mean_Q_m3s      = round(mean(Q_model), 3),
    median_Q_m3s    = round(median(Q_model), 3),
    max_Q_m3s       = round(max(Q_model), 3),
    min_Q_m3s       = round(min(Q_model), 3),
    mean_Q_spec     = round(mean(Q_spec), 2),
    annual_vol_mm   = round(sum(Q_model * timestep_s) /
                            (catchment_area_km2 * 1e6) * 1000, 1),
    .groups = "drop"
  ) |>
  left_join(quality_summary |> dplyr::select(water_year, pct_na),
            by = "water_year")

write_csv(summary_stats,
          file.path(out_dir, "ssn703_discharge_summary_stats.csv"))
message("Saved: ssn703_discharge_summary_stats.csv")

message("\n=== Summary statistics ===")
print(summary_stats)

message("\nDone. All outputs saved to ", out_dir)


discharge_combined |>
  filter(!is.na(Q_model)) |>
  pivot_longer(cols = c(stage_cm, Q_model),
               names_to = "variable", values_to = "value") |>
  mutate(variable = factor(variable,
                           levels = c("stage_cm", "Q_model"),
                           labels = c("Stage (cm)", "Discharge (m³/s)"))) |>
  ggplot(aes(x = timestamp, y = value, colour = discharge_flag)) +
  geom_line(linewidth = 0.3, alpha = 0.7) +
  facet_wrap(~ variable, ncol = 1, scales = "free_y") +
  scale_colour_manual(values = c(
    "raw"             = "#333333",
    "gf_spline"       = "#4DAF4A",
    "gf_spline_event" = "#FF7F00",
    "unfilled"        = "#E41A1C",
    "out_of_range"    = "#984EA3"
  )) +
  theme_bw() +
  xlab(NULL) + ylab(NULL) +
  labs(title = "SSN703 -- stage and discharge timeseries",
       colour = "Flag")

