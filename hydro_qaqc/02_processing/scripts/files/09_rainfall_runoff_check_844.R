# =============================================================================
# SSN844 Rainfall-Runoff Check -- Script 09
# =============================================================================
# Purpose:
#   Sanity check: compare computed discharge against rainfall for SSN844.
#   Computes annual runoff coefficients and cumulative double-mass curves
#   for RC2 (Rating 2, 2017-07-13 onward).
#
# Catchment area: 5.71 km²
# Rain gauge: rain_hecate (300m ASL)
# RC2 applied from: 2017-07-13
#
# Inputs:
#   04_outputs/ssn844_discharge_combined.csv
#   01_raw/SSN844/rain_hecate.csv
#
# Outputs:
#   04_outputs/ssn844_RC2_runoff_coefficients.png
#   04_outputs/ssn844_RC2_cumulative_rainfall_runoff.png
#   04_outputs/ssn844_RC2_monthly_rainfall_runoff.png
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)

select <- dplyr::select

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

CATCHMENT_AREA_M2 <- 5.71e6   # 5.71 km² in m²
RC2_START         <- as.POSIXct("2017-07-13", tz = "Etc/GMT+8")
TIMESTEP_SECS     <- 300

water_year_label <- function(dt) {
  yr <- year(dt); mo <- month(dt)
  wy <- ifelse(mo >= 10, yr, yr - 1)
  paste0(wy, "-", wy + 1)
}

# -----------------------------------------------------------------------------
# 1. Load discharge
# -----------------------------------------------------------------------------

discharge <- read_csv("04_outputs/ssn844_discharge_combined.csv",
                      show_col_types = FALSE) |>
  mutate(
    timestamp = as.POSIXct(timestamp, tz = "Etc/GMT+8"),
    Q_mm      = Q_model * TIMESTEP_SECS / CATCHMENT_AREA_M2 * 1000
  ) |>
  filter(timestamp >= RC2_START,
         !is.na(Q_model),
         qc_flag != "unfilled")

message("Discharge rows loaded: ", nrow(discharge))
message("Date range: ", min(discharge$timestamp), " to ", max(discharge$timestamp))

# -----------------------------------------------------------------------------
# 2. Load rainfall
# -----------------------------------------------------------------------------

rain_raw <- read_csv("01_raw/SSN844/rain_hecate.csv",
                     skip = 3,
                     show_col_types = FALSE) |>
  rename(timestamp = 1, rain_mm = 2) |>
  mutate(
    timestamp = as.POSIXct(timestamp, format = "%m/%d/%Y %H:%M",
                           tz = "Etc/GMT+8")
  ) |>
  filter(!is.na(rain_mm), !is.na(timestamp)) |>
  select(timestamp, rain_mm)

rain <- rain_raw |> filter(timestamp >= RC2_START)

message("Rain rows loaded: ", nrow(rain))

# -----------------------------------------------------------------------------
# 3. Align to 5-minute timestamps and join
# -----------------------------------------------------------------------------

discharge_5min <- discharge |>
  mutate(timestamp_5min = floor_date(timestamp, "5 minutes")) |>
  group_by(timestamp_5min, water_year) |>
  summarise(Q_mm = mean(Q_mm, na.rm = TRUE), .groups = "drop")

rain_5min <- rain |>
  mutate(timestamp_5min = floor_date(timestamp, "5 minutes")) |>
  group_by(timestamp_5min) |>
  summarise(rain_mm = sum(rain_mm, na.rm = TRUE), .groups = "drop")

combined <- full_join(
  discharge_5min |> group_by(timestamp_5min) |>
    summarise(Q_mm = sum(Q_mm, na.rm = TRUE), .groups = "drop"),
  rain_5min, by = "timestamp_5min"
) |>
  arrange(timestamp_5min) |>
  mutate(
    Q_mm    = replace_na(Q_mm, 0),
    rain_mm = replace_na(rain_mm, 0),
    water_year = water_year_label(timestamp_5min)
  )

# -----------------------------------------------------------------------------
# 4. Annual runoff coefficients
# -----------------------------------------------------------------------------

# Flag water years with > 10% NA discharge
na_by_wy <- discharge |>
  mutate(water_year = water_year_label(timestamp)) |>
  group_by(water_year) |>
  summarise(
    total = n(),
    n_na  = sum(is.na(Q_model)),
    pct_na = round(n_na / total * 100, 1),
    .groups = "drop"
  )

annual <- combined |>
  group_by(water_year) |>
  summarise(
    total_rain_mm   = sum(rain_mm, na.rm = TRUE),
    total_runoff_mm = sum(Q_mm,    na.rm = TRUE),
    runoff_coeff    = total_runoff_mm / total_rain_mm,
    .groups = "drop"
  ) |>
  left_join(na_by_wy |> select(water_year, pct_na), by = "water_year") |>
  mutate(
    pct_na    = replace_na(pct_na, 0),
    reliable  = pct_na <= 10
  )

message("\n=== RC2 Annual runoff coefficients ===")
print(annual)

p1 <- ggplot(annual, aes(x = water_year, y = runoff_coeff, fill = reliable)) +
  geom_col() +
  geom_hline(yintercept = 1,   linetype = "dashed", colour = "red") +
  geom_hline(yintercept = 0.7, linetype = "dotted", colour = "grey50") +
  scale_fill_manual(values = c("TRUE" = "#4E9BB9", "FALSE" = "grey70"),
                    labels = c("TRUE" = "≤10% NA", "FALSE" = ">10% NA")) +
  theme_bw() +
  xlab("Water year") + ylab("Runoff coefficient (Q/P)") +
  labs(title = "SSN844 RC2 -- annual runoff coefficients",
       subtitle = "Dashed red = 1.0 | Dotted = 0.7 (reference)",
       fill = "Data coverage",
       caption = paste0("rain_hecate gauge (300m ASL) | RC2 start: 2017-07-13\n",
                        "Grey bars = water years with >10% NA discharge"))

print(p1)
ggsave("04_outputs/ssn844_RC2_runoff_coefficients.png", p1, width = 10, height = 6)
message("Saved: ssn844_RC2_runoff_coefficients.png")

# -----------------------------------------------------------------------------
# 5. Cumulative double mass
# -----------------------------------------------------------------------------

cumulative <- combined |>
  group_by(water_year) |>
  arrange(timestamp_5min) |>
  mutate(
    cum_rain_mm   = cumsum(rain_mm),
    cum_runoff_mm = cumsum(Q_mm)
  ) |>
  ungroup()

p2 <- ggplot(cumulative,
             aes(x = cum_rain_mm, y = cum_runoff_mm, colour = water_year)) +
  geom_line() +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", colour = "red") +
  theme_bw() +
  xlab("Cumulative rainfall (mm)") + ylab("Cumulative runoff (mm)") +
  labs(title = "SSN844 RC2 -- cumulative rainfall vs runoff",
       subtitle = "Dashed red = 1:1 line",
       colour = "Water year")

print(p2)
ggsave("04_outputs/ssn844_RC2_cumulative_rainfall_runoff.png", p2,
       width = 10, height = 7)
message("Saved: ssn844_RC2_cumulative_rainfall_runoff.png")

# -----------------------------------------------------------------------------
# 6. Monthly rainfall vs runoff bar chart
# -----------------------------------------------------------------------------

monthly <- combined |>
  mutate(month = floor_date(timestamp_5min, "month")) |>
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

p3 <- ggplot(monthly, aes(x = month, y = mm, fill = variable)) +
  geom_col(position = "dodge") +
  scale_x_datetime(date_labels = "%b %Y", date_breaks = "3 months") +
  scale_fill_manual(values = c("Rainfall" = "#4E9BB9", "Runoff" = "#E07B54")) +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  xlab(NULL) + ylab("Total (mm)") +
  labs(title = "SSN844 RC2 -- monthly rainfall vs runoff",
       fill = NULL,
       caption = "rain_hecate gauge (300m ASL)")

print(p3)
ggsave("04_outputs/ssn844_RC2_monthly_rainfall_runoff.png", p3,
       width = 12, height = 6)
message("Saved: ssn844_RC2_monthly_rainfall_runoff.png")

message("\nDone. Outputs saved to 04_outputs/")
