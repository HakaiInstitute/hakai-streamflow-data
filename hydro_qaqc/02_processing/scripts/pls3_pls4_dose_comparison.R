# =============================================================================
# pls3_pls4_dose_comparison.R -- SSN703 PLS3 vs PLS4 agreement at DoseEvents
# =============================================================================
# Run from PROJECT ROOT:
#   source("02_processing/scripts/pls3_pls4_dose_comparison.R")
#
# Compares PLS3_Lvl / PLS4_Lvl against StreamHeightRelease / ...PlusFive /
# ...PlusTen from DoseEvent .dat exports spanning the SSN703US datalogger
# program change (dosing driven by PLS3+PLS4 pre-2021-09 vs PLS4-only after)
# and filling the gap up to PLS3's 2023-09-15 removal:
#   03_docs/metadata/SSN703US_DoseEvent_20210922.dat  (2020-11-24 -> 2021-09-21)
#   03_docs/metadata/SSN703US_DoseEvent_20220403.dat  (2021-10-16 -> 2022-04-03)
#   03_docs/metadata/SSN703US_DoseEvent_20221225.dat  (2022-07-11 -> 2022-12-25)
#   03_docs/metadata/SSN703US_DoseEvent_20251225.dat  (2025-09-12 -> 2025-12-15)
#
# No raw FiveMin_*.dat files exist in this repo. Per Emily (2026-09-14), the
# continuous PLS3_Lvl / PLS4_Lvl series used here are the Hakai API / EIMS
# 5-min exports at 01_raw/SSN703/ssn703_c.csv (PLS3) and ssn703_d.csv (PLS4)
# -- confirmed to cover both DoseEvent windows and to show the same sensor
# handoff (PLS3 empty in the 2025 window, PLS4 empty in the 2020-21 window).
#
# Timestamps: DoseEvent TIMESTAMP/DoseReleaseTS are the CR1000 logger's local
# clock (tz undocumented in the .dat header); ssn703_c/d.csv "Measurement
# time" is labeled PST. Per Emily, both are treated as fixed-standard-time
# (no DST) with zero offset between them -- parsed here as Etc/GMT+8.
# =============================================================================

library(tidyverse)

DOSE_FILES <- c(
  "03_docs/metadata/SSN703US_DoseEvent_20210922.dat",
  "03_docs/metadata/SSN703US_DoseEvent_20220403.dat",
  "03_docs/metadata/SSN703US_DoseEvent_20221225.dat",
  "03_docs/metadata/SSN703US_DoseEvent_20251225.dat"
)

PLS3_PATH <- "01_raw/SSN703/ssn703_c.csv"
PLS4_PATH <- "01_raw/SSN703/ssn703_d.csv"

TOL_M      <- 0.02          # 2 cm tolerance for "which sensor governed" call
MAX_GAP_S  <- 200           # max allowed distance to nearest 5-min sample
TZ         <- "Etc/GMT+8"   # fixed PST, no DST (see header note above)

OUT_DIR <- "04_outputs/pls3_pls4_comparison"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# -----------------------------------------------------------------------------
# 1. Parsers -- CRBasic TOA5 (.dat): row1 station info, row2 field names,
#    row3 process codes, row4 units, data from row5. Hakai/EIMS .csv: row1
#    display names, row2 units, row3 station repeat, row4 field names, data
#    from row5.
# -----------------------------------------------------------------------------
read_dose_event <- function(path) {
  nms <- names(read_csv(path, skip = 1, n_max = 0, show_col_types = FALSE))
  df <- read_csv(path, skip = 4, col_names = nms, show_col_types = FALSE,
                  col_types = cols(.default = "c"))
  df %>%
    mutate(
      source_file          = basename(path),
      TIMESTAMP            = as.POSIXct(TIMESTAMP, format = "%Y-%m-%d %H:%M:%OS", tz = TZ),
      DoseReleaseTS        = as.POSIXct(DoseReleaseTS, format = "%m/%d/%Y %H:%M:%OS", tz = TZ),
      DoseEventStartTS     = as.POSIXct(DoseEventStartTS, format = "%m/%d/%Y %H:%M:%OS", tz = TZ),
      DoseEventID          = as.numeric(DoseEventID),
      StreamHeightRelease          = as.numeric(StreamHeightRelease),
      StreamHeightReleasePlusFive  = as.numeric(StreamHeightReleasePlusFive),
      StreamHeightReleasePlusTen   = as.numeric(StreamHeightReleasePlusTen),
      release_plus5_ts     = DoseReleaseTS + 5 * 60,
      release_plus10_ts    = DoseReleaseTS + 10 * 60
    ) %>%
    select(source_file, DoseEventID, TIMESTAMP, DoseReleaseTS, release_plus5_ts,
           release_plus10_ts, StreamHeightRelease, StreamHeightReleasePlusFive,
           StreamHeightReleasePlusTen)
}

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

# -----------------------------------------------------------------------------
# 2. Nearest-neighbour lookup on the 5-min grid
# -----------------------------------------------------------------------------
nearest_value <- function(series, target, max_gap_s = MAX_GAP_S) {
  # series: data.frame(measurementTime, value), sorted; target: single POSIXct
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
# 3. Load everything
# -----------------------------------------------------------------------------
dose <- map_dfr(DOSE_FILES, read_dose_event)
pls3 <- read_pls_series(PLS3_PATH, "PLS3_Lvl")
pls4 <- read_pls_series(PLS4_PATH, "PLS4_Lvl")

# -----------------------------------------------------------------------------
# 4. Attach nearest PLS3 / PLS4 values at release, +5, +10
# -----------------------------------------------------------------------------
p3_rel <- vnearest(pls3, dose$DoseReleaseTS)
p4_rel <- vnearest(pls4, dose$DoseReleaseTS)
p3_p5  <- vnearest(pls3, dose$release_plus5_ts)
p4_p5  <- vnearest(pls4, dose$release_plus5_ts)
p3_p10 <- vnearest(pls3, dose$release_plus10_ts)
p4_p10 <- vnearest(pls4, dose$release_plus10_ts)

result <- dose %>%
  mutate(
    PLS3_Lvl_release = p3_rel$value, PLS3_release_gap_s = p3_rel$gap_s,
    PLS4_Lvl_release = p4_rel$value, PLS4_release_gap_s = p4_rel$gap_s,
    PLS3_Lvl_plus5    = p3_p5$value,  PLS3_plus5_gap_s   = p3_p5$gap_s,
    PLS4_Lvl_plus5    = p4_p5$value,  PLS4_plus5_gap_s   = p4_p5$gap_s,
    PLS3_Lvl_plus10   = p3_p10$value, PLS3_plus10_gap_s  = p3_p10$gap_s,
    PLS4_Lvl_plus10   = p4_p10$value, PLS4_plus10_gap_s  = p4_p10$gap_s
  )

# -----------------------------------------------------------------------------
# 5. Which sensor governed? Diffs / pct diffs at each mark
# -----------------------------------------------------------------------------
match_sensor <- function(target, pls3_val, pls4_val, tol = TOL_M) {
  err3 <- abs(target - pls3_val)
  err4 <- abs(target - pls4_val)
  case_when(
    is.na(target)            ~ NA_character_,
    is.na(pls3_val) & is.na(pls4_val) ~ "neither (no sensor data)",
    is.na(pls3_val)           ~ if_else(err4 <= tol, "PLS4", "PLS4 (out of tol)"),
    is.na(pls4_val)           ~ if_else(err3 <= tol, "PLS3", "PLS3 (out of tol)"),
    err3 <= tol & err4 > tol  ~ "PLS3",
    err4 <= tol & err3 > tol  ~ "PLS4",
    err3 <= tol & err4 <= tol ~ "ambiguous (both within tol)",
    TRUE                      ~ "neither within tol"
  )
}

add_diff_cols <- function(df, suffix, target_col) {
  p3 <- df[[paste0("PLS3_Lvl_", suffix)]]
  p4 <- df[[paste0("PLS4_Lvl_", suffix)]]
  tgt <- df[[target_col]]
  df[[paste0("matched_sensor_", suffix)]] <- match_sensor(tgt, p3, p4)
  df[[paste0("diff_", suffix, "_m")]]     <- p3 - p4
  df[[paste0("pct_diff_", suffix)]]       <- (p3 - p4) / ((p3 + p4) / 2) * 100
  df
}

result <- result %>%
  add_diff_cols("release", "StreamHeightRelease") %>%
  add_diff_cols("plus5",   "StreamHeightReleasePlusFive") %>%
  add_diff_cols("plus10",  "StreamHeightReleasePlusTen")

# -----------------------------------------------------------------------------
# 6. Write summary CSV
# -----------------------------------------------------------------------------
out_csv <- file.path(OUT_DIR, "ssn703_pls3_pls4_dose_comparison.csv")
write_csv(result, out_csv)
message("Wrote ", out_csv, " (", nrow(result), " dose events)")

# -----------------------------------------------------------------------------
# 7. Summary stats per file, per mark
# -----------------------------------------------------------------------------
stats <- result %>%
  select(source_file, starts_with("diff_")) %>%
  pivot_longer(starts_with("diff_"), names_to = "mark", values_to = "diff_m") %>%
  mutate(mark = str_remove(mark, "^diff_") %>% str_remove("_m$")) %>%
  filter(!is.na(diff_m)) %>%
  group_by(source_file, mark) %>%
  summarise(
    n        = n(),
    mean_m   = mean(diff_m),
    max_abs_m = max(abs(diff_m)),
    sd_m     = sd(diff_m),
    .groups = "drop"
  )

out_stats <- file.path(OUT_DIR, "ssn703_pls3_pls4_dose_comparison_summary_stats.csv")
write_csv(stats, out_stats)
message("Wrote ", out_stats)
print(stats)

sensor_match_counts <- result %>%
  count(source_file, matched_sensor_release, name = "n_dose_events")
message("\nSensor governing StreamHeightRelease, by file:")
print(sensor_match_counts)

# -----------------------------------------------------------------------------
# 8. Time-series plots per file: PLS3_Lvl / PLS4_Lvl with dose events marked
# -----------------------------------------------------------------------------
plot_file_window <- function(fname) {
  d <- result %>% filter(source_file == fname)
  rng <- range(d$DoseReleaseTS, na.rm = TRUE) + c(-1, 1) * 86400  # +/-1 day pad

  p3_win <- pls3 %>% filter(measurementTime >= rng[1], measurementTime <= rng[2]) %>%
    mutate(sensor = "PLS3")
  p4_win <- pls4 %>% filter(measurementTime >= rng[1], measurementTime <= rng[2]) %>%
    mutate(sensor = "PLS4")
  series <- bind_rows(p3_win, p4_win)

  p <- ggplot() +
    geom_line(data = series, aes(x = measurementTime, y = value, colour = sensor),
              linewidth = 0.4, na.rm = TRUE) +
    geom_vline(data = d, aes(xintercept = as.numeric(DoseReleaseTS)),
               linetype = "dashed", colour = "grey40", linewidth = 0.3) +
    geom_point(data = d, aes(x = DoseReleaseTS, y = StreamHeightRelease),
               shape = 4, size = 2, colour = "black") +
    scale_colour_manual(values = c(PLS3 = "#1b9e77", PLS4 = "#d95f02")) +
    labs(title = paste0("SSN703 PLS3 vs PLS4 -- ", fname),
         subtitle = "Dashed lines = DoseEvent release; X = StreamHeightRelease",
         x = NULL, y = "Stream level (m)", colour = "Sensor") +
    theme_minimal()

  out_png <- file.path(OUT_DIR, paste0(tools::file_path_sans_ext(fname), "_timeseries.png"))
  ggsave(out_png, p, width = 12, height = 5, dpi = 150)
  message("Wrote ", out_png)
}

walk(unique(result$source_file), plot_file_window)
