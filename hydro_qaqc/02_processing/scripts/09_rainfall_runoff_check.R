# =============================================================================
# 09_rainfall_runoff_check.R
# Sanity check: compare computed discharge against rainfall for SSN703
# Catchment area: 12.79 km²
# RC2 applied from: 2019-02-09
# RC3 applied from: 2023-09-15
# =============================================================================

library(tidyverse)

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
CATCHMENT_AREA_M2 <- 12.79e6  # 12.79 km² in m²
RC2_START         <- as.POSIXct("2019-02-09", tz = "UTC")
RC3_START         <- as.POSIXct("2023-09-15", tz = "UTC")

# -----------------------------------------------------------------------------
# Load stage data
# -----------------------------------------------------------------------------
stage_RC2 <- read_csv("04_outputs/per_rc/ssn703_RC2_stage_qc.csv") |>
  mutate(timestamp = as.POSIXct(timestamp, tz = "UTC")) |>
  filter(timestamp >= RC2_START, timestamp < RC3_START)

stage_RC3 <- read_csv("04_outputs/per_rc/ssn703_RC3_stage_qc.csv") |>
  mutate(timestamp = as.POSIXct(timestamp, tz = "UTC")) |>
  filter(timestamp >= RC3_START)

stage_all <- bind_rows(stage_RC2, stage_RC3)

# -----------------------------------------------------------------------------
# Load rating curve lookup tables
# -----------------------------------------------------------------------------
rc2_lookup <- read_csv("04_outputs/ssn703_RC2_rating_curve_v1.csv") |>
  dplyr::select(Stage_avg, Q_model)

rc3_lookup <- read_csv("04_outputs/ssn703_RC3_rating_curve_v1.csv") |>
  dplyr::select(Stage_avg, Q_model)

# -----------------------------------------------------------------------------
# Apply rating curves -- interpolate Q from stage
# -----------------------------------------------------------------------------
apply_rating <- function(stage_vec, lookup) {
  approx(x = lookup$Stage_avg, y = lookup$Q_model,
         xout = stage_vec, rule = 1)$y
}

discharge <- stage_all |>
  mutate(
    stage_cm = stage_corrected * 100,
    Q_m3s = case_when(
      rating_curve_period == "RC2" ~ apply_rating(stage_cm, rc2_lookup),
      rating_curve_period == "RC3" ~ apply_rating(stage_cm, rc3_lookup),
      TRUE ~ NA_real_
    )
  ) |>
  filter(!is.na(Q_m3s), qc_flag != "bad_data")

# Hardcoded timestep -- stage data is 5-minute resolution
timestep_secs <- 300

# Convert discharge to mm over catchment per timestep
discharge <- discharge |>
  mutate(Q_mm = Q_m3s * timestep_secs / CATCHMENT_AREA_M2 * 1000)

# -----------------------------------------------------------------------------
# Load rainfall -- 600m gauge preferred for orographic representation
# -----------------------------------------------------------------------------
rain_raw <- read_csv("01_raw/SSN703/rain/ssn693703_rain.csv", skip = 3) |>
  rename(timestamp = measurementTime, rain_mm = Rain) |>
  mutate(timestamp = as.POSIXct(timestamp, format = "%m/%d/%Y %H:%M", tz = "PST")) |>
  mutate(timestamp = with_tz(timestamp, "UTC")) |>
  filter(!is.na(rain_mm), !is.na(timestamp)) |>
  dplyr::select(timestamp, rain_mm)

rain <- rain_raw |> filter(timestamp >= RC2_START)

# -----------------------------------------------------------------------------
# Align to 5-minute timestamps
# -----------------------------------------------------------------------------
discharge_5min <- discharge |>
  mutate(timestamp_5min = floor_date(timestamp, unit = "5 minutes")) |>
  group_by(timestamp_5min, rating_curve_period) |>
  summarise(Q_mm = mean(Q_mm, na.rm = TRUE), .groups = "drop")

rain_5min <- rain |>
  mutate(timestamp_5min = floor_date(timestamp, unit = "5 minutes")) |>
  group_by(timestamp_5min) |>
  summarise(rain_mm = sum(rain_mm, na.rm = TRUE), .groups = "drop")

combined <- full_join(
  discharge_5min |> group_by(timestamp_5min) |>
    summarise(Q_mm = sum(Q_mm, na.rm = TRUE), .groups = "drop"),
  rain_5min, by = "timestamp_5min") |>
  arrange(timestamp_5min) |>
  mutate(
    Q_mm    = replace_na(Q_mm, 0),
    rain_mm = replace_na(rain_mm, 0),
    water_year = case_when(
      month(timestamp_5min) >= 10 ~ paste0(year(timestamp_5min), "-",
                                            year(timestamp_5min) + 1),
      TRUE                        ~ paste0(year(timestamp_5min) - 1, "-",
                                            year(timestamp_5min))
    )
  )

# =============================================================================
# RC2 VALIDATION -- Oct 2019 to Jun 2023
# =============================================================================
combined_RC2 <- combined |>
  filter(timestamp_5min >= as.POSIXct("2019-10-01", tz = "UTC"),
         timestamp_5min <  as.POSIXct("2023-06-25", tz = "UTC"))

annual_RC2 <- combined_RC2 |>
  group_by(water_year) |>
  summarise(
    total_rain_mm   = sum(rain_mm, na.rm = TRUE),
    total_runoff_mm = sum(Q_mm,    na.rm = TRUE),
    runoff_coeff    = total_runoff_mm / total_rain_mm,
    .groups = "drop"
  )

message("=== RC2 Annual runoff coefficients ===")
print(annual_RC2)

# Plot RC2 -- annual runoff coefficients
p1 <- ggplot(annual_RC2, aes(x = water_year, y = runoff_coeff)) +
  geom_col(fill = "#4E9BB9") +
  geom_hline(yintercept = 1,   linetype = "dashed", colour = "red") +
  geom_hline(yintercept = 0.7, linetype = "dotted", colour = "grey50") +
  theme_bw() +
  xlab("Water year") + ylab("Runoff coefficient (Q/P)") +
  labs(title = "SSN703 RC2 -- annual runoff coefficients",
       subtitle = "Dashed red = 1.0; Dotted = 0.7 (historical average)",
       caption = "600m gauge | Oct 2019 to Jun 2023")
print(p1)
ggsave("04_outputs/ssn703_RC2_runoff_coefficients.png", p1, width = 10, height = 6)

# Plot RC2 -- cumulative double mass
cumulative_RC2 <- combined_RC2 |>
  group_by(water_year) |>
  arrange(timestamp_5min) |>
  mutate(cum_rain_mm   = cumsum(rain_mm),
         cum_runoff_mm = cumsum(Q_mm)) |>
  ungroup()

p2 <- ggplot(cumulative_RC2, aes(x = cum_rain_mm, y = cum_runoff_mm,
                                  colour = water_year)) +
  geom_line() +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", colour = "red") +
  theme_bw() +
  xlab("Cumulative rainfall (mm)") + ylab("Cumulative runoff (mm)") +
  labs(title = "SSN703 RC2 -- cumulative rainfall vs runoff",
       subtitle = "Dashed red = 1:1 line",
       colour = "Water year")
print(p2)
ggsave("04_outputs/ssn703_RC2_cumulative_rainfall_runoff.png", p2,
       width = 10, height = 7)

# =============================================================================
# RC3 VALIDATION -- Oct 2023 onward
# =============================================================================

# Check data coverage per water year before computing coefficients
rc3_coverage <- discharge |>
  filter(rating_curve_period == "RC3") |>
  mutate(timestamp_5min = floor_date(timestamp, unit = "5 minutes")) |>
  group_by(water_year) |>
  summarise(
    total     = n(),
    n_na      = sum(is.na(Q_m3s)),
    pct_na    = round(n_na / total * 100, 1),
    .groups   = "drop"
  ) |>
  mutate(reliable = pct_na <= 10)

message("=== RC3 data coverage by water year ===")
print(rc3_coverage)

unreliable_years <- rc3_coverage |> filter(!reliable) |> pull(water_year)
if (length(unreliable_years) > 0) {
  message("WARNING: following RC3 water years have >10% NA discharge and should",
          " not be used for runoff coefficient interpretation: ",
          paste(unreliable_years, collapse = ", "))
}

combined_RC3 <- combined |>
  filter(timestamp_5min >= as.POSIXct("2023-10-01", tz = "UTC"))

annual_RC3 <- combined_RC3 |>
  group_by(water_year) |>
  summarise(
    total_rain_mm   = sum(rain_mm, na.rm = TRUE),
    total_runoff_mm = sum(Q_mm,    na.rm = TRUE),
    runoff_coeff    = total_runoff_mm / total_rain_mm,
    .groups = "drop"
  ) |>
  left_join(rc3_coverage |> dplyr::select(water_year, pct_na, reliable),
            by = "water_year")

message("=== RC3 Annual runoff coefficients ===")
print(annual_RC3)

# Plot RC3 -- annual runoff coefficients
# Unreliable years shown in grey with annotation
p3 <- ggplot(annual_RC3,
             aes(x = water_year, y = runoff_coeff,
                 fill = reliable)) +
  geom_col() +
  geom_text(data = annual_RC3 |> filter(!reliable),
            aes(label = paste0(pct_na, "% data gap")),
            vjust = -0.5, size = 3, colour = "grey40") +
  geom_hline(yintercept = 1,   linetype = "dashed", colour = "red") +
  geom_hline(yintercept = 0.7, linetype = "dotted", colour = "grey50") +
  scale_fill_manual(values = c("TRUE" = "#7BAF9E", "FALSE" = "grey70"),
                    labels = c("TRUE" = "Reliable (< 10% NA)",
                               "FALSE" = "Unreliable (> 10% NA)")) +
  theme_bw() +
  xlab("Water year") + ylab("Runoff coefficient (Q/P)") +
  labs(title = "SSN703 RC3 -- annual runoff coefficients",
       subtitle = "Dashed red = 1.0; Dotted = 0.7 (historical average)",
       fill = "Data coverage",
       caption = paste0("600m gauge | Oct 2023 onward | Caution: provisional curve, sparse gaugings\n",
                        "Grey bars = water years with >10% NA discharge\n",
                        "RC3 curve extended below minimum gauged stage (56.88cm) via power law extrapolation (±20% CI)"))
print(p3)
ggsave("04_outputs/ssn703_RC3_runoff_coefficients.png", p3, width = 10, height = 6)

# Plot RC3 -- cumulative double mass
cumulative_RC3 <- combined_RC3 |>
  group_by(water_year) |>
  arrange(timestamp_5min) |>
  mutate(cum_rain_mm   = cumsum(rain_mm),
         cum_runoff_mm = cumsum(Q_mm)) |>
  ungroup()

p4 <- ggplot(cumulative_RC3, aes(x = cum_rain_mm, y = cum_runoff_mm,
                                  colour = water_year)) +
  geom_line() +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", colour = "red") +
  theme_bw() +
  xlab("Cumulative rainfall (mm)") + ylab("Cumulative runoff (mm)") +
  labs(title = "SSN703 RC3 -- cumulative rainfall vs runoff",
       subtitle = "Dashed red = 1:1 line | Caution: provisional curve",
       colour = "Water year")
print(p4)
ggsave("04_outputs/ssn703_RC3_cumulative_rainfall_runoff.png", p4,
       width = 10, height = 7)

# Plot RC3 -- monthly bar chart
monthly_RC3 <- combined_RC3 |>
  mutate(month = floor_date(timestamp_5min, unit = "month")) |>
  group_by(month) |>
  summarise(
    total_rain_mm   = sum(rain_mm, na.rm = TRUE),
    total_runoff_mm = sum(Q_mm,    na.rm = TRUE),
    .groups = "drop"
  ) |>
  pivot_longer(cols = c(total_rain_mm, total_runoff_mm),
               names_to = "variable", values_to = "mm") |>
  mutate(variable = recode(variable,
                           "total_rain_mm"   = "Rainfall",
                           "total_runoff_mm" = "Runoff"))

p5 <- ggplot(monthly_RC3, aes(x = month, y = mm, fill = variable)) +
  geom_col(position = "dodge") +
  scale_x_datetime(date_labels = "%b %Y", date_breaks = "1 month") +
  scale_fill_manual(values = c("Rainfall" = "#4E9BB9", "Runoff" = "#E07B54")) +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  xlab(NULL) + ylab("Total (mm)") +
  labs(title = "SSN703 RC3 -- monthly rainfall vs runoff",
       fill = NULL,
       caption = "Caution: provisional curve, sparse gaugings")
print(p5)
ggsave("04_outputs/ssn703_RC3_monthly_rainfall_runoff.png", p5,
       width = 12, height = 6)

message("Done. Outputs saved to 04_outputs/")
