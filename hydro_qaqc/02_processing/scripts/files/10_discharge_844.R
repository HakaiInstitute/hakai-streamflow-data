# =============================================================================
# SSN844 Discharge Computation -- Script 10
# =============================================================================
# Purpose:
#   Apply rating curves to QC'd stage data to produce a discharge timeseries
#   for SSN844.
#
#   RC1 (Rating 1): 2012-01-01 to 2017-07-12 -- previous pipeline, not recomputed
#   RC2 (Rating 2): 2017-07-13 onward, loc_1, ssn844 (extended to 160cm)
#
#   Only RC2 is computed here. RC1 discharge is already in the database and
#   is not recomputed.
#
# Stage units:
#   stage_corrected in QC files is in METRES
#   Rating curve lookup (Stage_avg) is in CM
#   Conversion: stage_corrected * 100 = stage_cm
#
# Gap handling:
#   qc_flag == "unfilled" --> discharge set to NA
#   All other flags (raw, gf_spline, gf_spline_event etc) --> discharge computed
#
# Extrapolation handling:
#   Stage below rating curve minimum --> discharge = NA
#   Stage above rating curve maximum (160cm) --> discharge = NA
#   approx() rule = 1 enforces this
#
# Inputs:
#   02_processing/data_output/per_rc/ssn844_RC2_stage_qc.csv
#   04_outputs/ssn844_rating_curve_lookup_combined.csv
#
# Outputs:
#   04_outputs/ssn844_discharge_RC2.csv
#   04_outputs/ssn844_discharge_combined.csv
#   02_processing/plots/ssn844_discharge_overview.pdf
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)

select  <- dplyr::select
out_dir <- "04_outputs"
per_rc  <- "02_processing/data_output/per_rc"
plot_dir <- "02_processing/plots"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)

# -----------------------------------------------------------------------------
# 1. Load rating curve lookup
# -----------------------------------------------------------------------------

lookup <- read_csv(
  file.path(out_dir, "ssn844_rating_curve_lookup_combined.csv"),
  show_col_types = FALSE
)

# RC2 = Rating 2 (extended to 160cm)
rc2 <- lookup |> filter(Rating == 2)

message("Rating 2 stage range: ", round(min(rc2$Stage_avg), 1),
        " -- ", round(max(rc2$Stage_avg), 1), " cm")
message("Rating 2 rows: ", nrow(rc2))

# -----------------------------------------------------------------------------
# 2. Helper function -- apply rating curve via approx()
# -----------------------------------------------------------------------------

apply_rating <- function(stage_cm, rc) {
  tibble(
    Q_model = approx(rc$Stage_avg, rc$Q_model,
                     xout = stage_cm, rule = 1)$y,
    Max_CI  = approx(rc$Stage_avg, rc$Max_CI,
                     xout = stage_cm, rule = 1)$y,
    Min_CI  = approx(rc$Stage_avg, rc$Min_CI,
                     xout = stage_cm, rule = 1)$y
  )
}

# -----------------------------------------------------------------------------
# 3. RC2 discharge
# -----------------------------------------------------------------------------

message("\nProcessing RC2...")

stage_rc2 <- read_csv(
  file.path(per_rc, "ssn844_RC2_stage_qc.csv"),
  show_col_types = FALSE
) |>
  mutate(timestamp = as.POSIXct(timestamp, tz = "Etc/GMT+8"))

message("RC2 rows: ", nrow(stage_rc2))
message("RC2 date range: ", min(stage_rc2$timestamp),
        " to ", max(stage_rc2$timestamp))
message("RC2 NA stage: ", sum(is.na(stage_rc2$stage_corrected)))

discharge_rc2 <- stage_rc2 |>
  mutate(stage_cm = stage_corrected * 100) |>
  bind_cols(apply_rating(
    stage_cm = ifelse(stage_rc2$qc_flag == "unfilled", NA,
                      stage_rc2$stage_corrected * 100),
    rc = rc2
  )) |>
  mutate(
    rating        = 2L,
    rating_label  = "RC2 (Rating 2) -- loc_1, ssn844",
    stage_source  = "ssn844 (loc_1)",
    discharge_flag = case_when(
      qc_flag == "unfilled" ~ "unfilled",
      is.na(Q_model)        ~ "out_of_range",
      TRUE                  ~ qc_flag
    )
  ) |>
  select(
    station_id, site_id, location_id, rating_curve_period,
    timestamp, water_year, stage_cm, stage_source, qc_flag,
    Q_model, Max_CI, Min_CI, rating, rating_label, discharge_flag
  )

message("RC2 discharge rows: ", nrow(discharge_rc2))
message("RC2 NA discharge: ", sum(is.na(discharge_rc2$Q_model)),
        " of ", nrow(discharge_rc2),
        " (", round(mean(is.na(discharge_rc2$Q_model)) * 100, 1), "%)")
message("RC2 out_of_range: ",
        sum(discharge_rc2$discharge_flag == "out_of_range", na.rm = TRUE))
message("RC2 unfilled: ",
        sum(discharge_rc2$discharge_flag == "unfilled", na.rm = TRUE))

write_csv(discharge_rc2, file.path(out_dir, "ssn844_discharge_RC2.csv"))
message("Saved: ssn844_discharge_RC2.csv")

# -----------------------------------------------------------------------------
# 4. Combined discharge timeseries
# -----------------------------------------------------------------------------
# Only RC2 is recomputed here. RC1 discharge (pre 2017-07-13) is not included
# as it was handled by the previous pipeline and is already in the database.

discharge_combined <- discharge_rc2 |>
  arrange(timestamp)

write_csv(discharge_combined,
          file.path(out_dir, "ssn844_discharge_combined.csv"))
message("\nSaved: ssn844_discharge_combined.csv")
message("Combined rows: ", nrow(discharge_combined))
message("Combined date range: ",
        min(discharge_combined$timestamp), " to ",
        max(discharge_combined$timestamp))

# -----------------------------------------------------------------------------
# 5. Discharge flag summary by water year
# -----------------------------------------------------------------------------

message("\n--- Discharge flag summary by water year ---")
discharge_combined |>
  group_by(water_year, rating_label) |>
  summarise(
    total         = n(),
    n_na          = sum(is.na(Q_model)),
    pct_na        = round(n_na / total * 100, 1),
    n_raw         = sum(discharge_flag == "raw",         na.rm = TRUE),
    n_gf_sa       = sum(discharge_flag == "gf_sa",       na.rm = TRUE),
    n_gf_spline   = sum(grepl("gf_spline", discharge_flag), na.rm = TRUE),
    n_out_range   = sum(discharge_flag == "out_of_range", na.rm = TRUE),
    n_unfilled    = sum(discharge_flag == "unfilled",     na.rm = TRUE),
    .groups = "drop"
  ) |>
  print(n = 30)

# -----------------------------------------------------------------------------
# 6. Summary check plot
# -----------------------------------------------------------------------------

p <- discharge_combined |>
  filter(!is.na(Q_model)) |>
  ggplot(aes(x = timestamp, y = Q_model)) +
  geom_line(data = ~ filter(., discharge_flag == "raw"),
            colour = "#333333", linewidth = 0.3, alpha = 0.7) +
  geom_point(data = ~ filter(., discharge_flag == "gf_sa"),
             colour = "#4DAF4A", size = 0.5) +
  geom_point(data = ~ filter(., discharge_flag == "gf_spline"),
             colour = "#74C2E1", size = 0.5) +
  geom_point(data = ~ filter(., discharge_flag == "gf_spline_event"),
             colour = "#FF7F00", size = 0.5) +
  theme_bw() +
  xlab(NULL) + ylab("Discharge (m³/s)") +
  labs(title = "SSN844 -- discharge timeseries (RC2, Rating 2)",
       subtitle = "NA values (unfilled gaps and out-of-range stage) not shown",
       caption = paste0(
         "Black = raw | Green = gf_sa | Blue = gf_spline | Orange = gf_spline_event\n",
         "Generated by 10_discharge_844.R"
       )) +
  theme(legend.position = "bottom")

pdf(file.path(plot_dir, "ssn844_discharge_overview.pdf"), width = 12, height = 6)
print(p)
dev.off()
message("Saved: ssn844_discharge_overview.pdf")

message("\nDone. Proceed to 09_rainfall_runoff_check_844.R")
