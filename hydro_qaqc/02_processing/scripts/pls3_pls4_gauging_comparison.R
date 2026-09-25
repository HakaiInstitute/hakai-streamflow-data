# =============================================================================
# pls3_pls4_gauging_comparison.R -- does Stage_avg (2021-09 to 2023-09) refer
# to PLS3 (ssn703_c) or PLS4 (ssn703_d)?
# =============================================================================
# Run from PROJECT ROOT.
#
# prep_gaugings.R currently assumes the whole RC2 period (2018-09-14 ->
# 2023-06-25, "suspect" through 2023-09-15) is on ssn703_c (PLS3) stage, with
# ssn703_d (PLS4) only starting in RC3 after 2023-09-15 ("db updated to
# ssn703_d"). The DoseEvent comparison (see [[ssn703-pls3-pls4-sensors]])
# showed the *dosing hardware* switched to referencing PLS4 almost
# immediately after its 2021-09-02 install. This script checks which sensor
# the gauging table's Stage_avg actually agrees with, gauging by gauging,
# across the 2021-09 -> 2023-09 window, using nearest-5-min PLS3_Lvl/PLS4_Lvl
# from ssn703_c.csv / ssn703_d.csv (same source as pls3_pls4_dose_comparison.R).
#
# Confirmed: ssn703_gaugings_prepped.csv's `datetime` column, despite its
# ISO8601 "Z" (UTC) suffix, matches DoseReleaseTS exactly to the second for
# shared events -- i.e. it's the same fixed-PST local clock, not real UTC.
# Stage_avg is in cm; PLS3_Lvl/PLS4_Lvl are in m.
# =============================================================================

library(tidyverse)

GAUGINGS_PATH <- "03_docs/metadata/ssn703_gaugings_prepped.csv"
PLS3_PATH <- "01_raw/SSN703/ssn703_c.csv"
PLS4_PATH <- "01_raw/SSN703/ssn703_d.csv"

WINDOW_START <- as.POSIXct("2021-09-01 00:00:00", tz = "Etc/GMT+8")
WINDOW_END   <- as.POSIXct("2023-09-20 00:00:00", tz = "Etc/GMT+8")

TOL_M     <- 0.02
MAX_GAP_S <- 200
TZ        <- "Etc/GMT+8"

OUT_DIR <- "04_outputs/pls3_pls4_comparison"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

read_pls_series <- function(path, var) {
  nms <- names(read_csv(path, skip = 3, n_max = 0, show_col_types = FALSE))
  df <- read_csv(path, skip = 4, col_names = nms, show_col_types = FALSE,
                  col_types = cols(.default = "c"))
  df %>%
    transmute(
      measurementTime = as.POSIXct(measurementTime, format = "%Y-%m-%d %H:%M:%S", tz = TZ),
      value = as.numeric(.data[[var]])
    ) %>%
    filter(!is.na(measurementTime)) %>%
    arrange(measurementTime)
}

nearest_value <- function(series, target, max_gap_s = MAX_GAP_S) {
  if (is.na(target) || nrow(series) == 0) return(c(value = NA_real_, gap_s = NA_real_))
  idx <- findInterval(target, series$measurementTime)
  cands <- unique(pmin(pmax(c(idx, idx + 1), 1), nrow(series)))
  gaps <- abs(as.numeric(difftime(series$measurementTime[cands], target, units = "secs")))
  best <- cands[which.min(gaps)]
  gap  <- gaps[which.min(gaps)]
  if (gap > max_gap_s) return(c(value = NA_real_, gap_s = gap))
  c(value = series$value[best], gap_s = gap)
}

vnearest <- function(series, targets) {
  out <- vapply(targets, function(t) nearest_value(series, t), numeric(2))
  list(value = out["value", ], gap_s = out["gap_s", ])
}

# -----------------------------------------------------------------------------
# Load
# -----------------------------------------------------------------------------
gaugings_raw <- read_csv(GAUGINGS_PATH, show_col_types = FALSE)

gaugings <- gaugings_raw %>%
  mutate(datetime_local = with_tz(datetime, TZ)) %>%  # datetime is genuine UTC; convert (not relabel) to local
  filter(datetime_local >= WINDOW_START, datetime_local <= WINDOW_END) %>%
  select(EventID, datetime_local, rating_curve_period, Stage_avg, Stage_avg_corrected,
         stage_source, stage_status, Q_meas)

pls3 <- read_pls_series(PLS3_PATH, "PLS3_Lvl")
pls4 <- read_pls_series(PLS4_PATH, "PLS4_Lvl")

p3 <- vnearest(pls3, gaugings$datetime_local)
p4 <- vnearest(pls4, gaugings$datetime_local)

result <- gaugings %>%
  mutate(
    Stage_avg_m   = Stage_avg / 100,
    PLS3_Lvl      = p3$value, PLS3_gap_s = p3$gap_s,
    PLS4_Lvl      = p4$value, PLS4_gap_s = p4$gap_s,
    err_PLS3      = abs(Stage_avg_m - PLS3_Lvl),
    err_PLS4      = abs(Stage_avg_m - PLS4_Lvl),
    matched_sensor = case_when(
      is.na(Stage_avg_m) ~ NA_character_,
      is.na(PLS3_Lvl) & is.na(PLS4_Lvl) ~ "neither (no sensor data)",
      is.na(PLS3_Lvl) ~ if_else(err_PLS4 <= TOL_M, "PLS4", "PLS4 (out of tol)"),
      is.na(PLS4_Lvl) ~ if_else(err_PLS3 <= TOL_M, "PLS3", "PLS3 (out of tol)"),
      err_PLS3 <= TOL_M & err_PLS4 > TOL_M ~ "PLS3",
      err_PLS4 <= TOL_M & err_PLS3 > TOL_M ~ "PLS4",
      err_PLS3 <= TOL_M & err_PLS4 <= TOL_M ~ "ambiguous (both within tol)",
      TRUE ~ "neither within tol"
    )
  )

out_csv <- file.path(OUT_DIR, "ssn703_gauging_pls3_pls4_comparison_2021-2023.csv")
write_csv(result, out_csv)
message("Wrote ", out_csv, " (", nrow(result), " gaugings)")

message("\nMatched sensor counts (all gaugings in window):")
print(result %>% count(matched_sensor))

message("\nMatched sensor counts by half-year bucket:")
print(result %>%
        mutate(half = paste0(year(datetime_local), "-H", if_else(month(datetime_local) <= 6, 1, 2))) %>%
        count(half, matched_sensor) %>%
        pivot_wider(names_from = matched_sensor, values_from = n, values_fill = 0))

message("\nMean/median |err| where Stage_avg and both sensors present:")
print(result %>% filter(!is.na(Stage_avg_m), !is.na(PLS3_Lvl), !is.na(PLS4_Lvl)) %>%
        summarise(n = n(),
                  mean_err_PLS3 = mean(err_PLS3), median_err_PLS3 = median(err_PLS3),
                  mean_err_PLS4 = mean(err_PLS4), median_err_PLS4 = median(err_PLS4),
                  n_PLS3_closer = sum(err_PLS3 < err_PLS4)))

# -----------------------------------------------------------------------------
# Plot: Stage_avg over the continuous PLS3/PLS4 series for the window
# -----------------------------------------------------------------------------
series <- bind_rows(
  pls3 %>% filter(measurementTime >= WINDOW_START, measurementTime <= WINDOW_END) %>% mutate(sensor = "PLS3"),
  pls4 %>% filter(measurementTime >= WINDOW_START, measurementTime <= WINDOW_END) %>% mutate(sensor = "PLS4")
)

p <- ggplot() +
  geom_line(data = series, aes(x = measurementTime, y = value, colour = sensor),
            linewidth = 0.3, na.rm = TRUE) +
  geom_point(data = result %>% filter(!is.na(Stage_avg_m)),
             aes(x = datetime_local, y = Stage_avg_m), shape = 4, size = 2, colour = "black") +
  scale_colour_manual(values = c(PLS3 = "#1b9e77", PLS4 = "#d95f02")) +
  labs(title = "SSN703 gauging Stage_avg vs PLS3/PLS4, 2021-09 to 2023-09",
       subtitle = "X = gauging Stage_avg (rating_curve_period = RC2 throughout)",
       x = NULL, y = "Stream level (m)", colour = "Sensor") +
  theme_minimal()

out_png <- file.path(OUT_DIR, "ssn703_gauging_pls3_pls4_comparison_2021-2023_timeseries.png")
ggsave(out_png, p, width = 14, height = 5, dpi = 150)
message("Wrote ", out_png)
