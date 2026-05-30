# =============================================================================
# SSN703 Discharge Computation -- Script 10
# =============================================================================
# Purpose:
#   Apply rating curves to QC'd stage data to produce a discharge timeseries
#   for the period 2019-02-09 onward (RC2 and RC3 only).
#
#   The discharge record prior to 2019-02-09 (Ratings 1-3, loc_1) is already
#   in the database and is not recomputed here.
#
# Rating curve periods:
#   RC2 (Rating 4): 2019-02-09 onward, loc_2, ssn703_c
#   RC3 (Rating 5): 2023-09-15 onward, loc_3, ssn703_d
#
# Stage units:
#   stage_corrected in QC files is in METRES
#   Rating curve lookup (Stage_avg) is in CM
#   Conversion: stage_corrected * 100 = stage_cm
#
# Gap handling:
#   qc_flag == "unfilled" --> discharge set to NA
#   All other flags (raw, gf_spline, gf_spline_event) --> discharge computed
#
# Extrapolation handling:
#   Stage below rating curve minimum --> discharge = NA
#   Stage above rating curve maximum --> discharge = NA
#   approx() rule = 1 enforces this
#
# Inputs:
#   04_outputs/per_rc/ssn703_RC2_stage_qc.csv
#   04_outputs/per_rc/ssn703_RC3_stage_qc.csv
#   04_outputs/ssn703_rating_curve_lookup_combined.csv
#
# Outputs:
#   04_outputs/ssn703_discharge_RC2.csv
#   04_outputs/ssn703_discharge_RC3.csv
#   04_outputs/ssn703_discharge_combined.csv
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)

out_dir  <- "04_outputs"
per_rc   <- file.path(out_dir, "per_rc")


# -----------------------------------------------------------------------------
# 1. Load rating curve lookup
# -----------------------------------------------------------------------------

lookup <- read_csv(
  file.path(out_dir, "ssn703_rating_curve_lookup_combined.csv"),
  show_col_types = FALSE
)

# Split by rating for clean approx() lookups
rc4 <- lookup |> filter(Rating == 4)  # RC2, loc_2
rc5 <- lookup |> filter(Rating == 5)  # RC3, loc_3

message("Rating 4 stage range: ", round(min(rc4$Stage_avg), 1),
        " -- ", round(max(rc4$Stage_avg), 1), " cm")
message("Rating 5 stage range: ", round(min(rc5$Stage_avg), 1),
        " -- ", round(max(rc5$Stage_avg), 1), " cm")


# -----------------------------------------------------------------------------
# 2. Helper function -- apply rating curve via approx()
# -----------------------------------------------------------------------------
# Interpolates Q_model, Max_CI, Min_CI from lookup table at each stage value.
# stage_cm: numeric vector in cm
# rc: rating curve lookup dataframe (Stage_avg, Q_model, Max_CI, Min_CI)
# Returns: dataframe with Q_model, Max_CI, Min_CI columns

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
  file.path(per_rc, "ssn703_RC2_stage_qc.csv"),
  show_col_types = FALSE
)

message("RC2 rows: ", nrow(stage_rc2))
message("RC2 date range: ", min(stage_rc2$timestamp), " to ", max(stage_rc2$timestamp))

# Filter to RC2 start date (2019-02-09) -- earlier stage exists but discharge
# for that period was handled by the previous pipeline
stage_rc2 <- stage_rc2 |>
  filter(timestamp >= as.POSIXct("2019-02-09 00:00:00", tz = "UTC"))

message("RC2 rows after 2019-02-09 filter: ", nrow(stage_rc2))

# Apply rating curve
discharge_rc2 <- stage_rc2 |>
  mutate(
    stage_cm = stage_corrected * 100,
    # set stage to NA where unfilled so approx returns NA
    stage_cm_for_rc = ifelse(qc_flag == "unfilled", NA, stage_cm)
  ) |>
  bind_cols(apply_rating(
    stage_cm = (\(x) ifelse(is.na(x), NA_real_, x))(
      ifelse(stage_rc2$qc_flag == "unfilled", NA,
             stage_rc2$stage_corrected * 100)
    ),
    rc = rc4
  )) |>
  mutate(
    rating        = 4L,
    rating_label  = "RC2 (Rating 4) -- loc_2, ssn703_c",
    stage_source  = "ssn703_c (loc_2)",
    discharge_flag = case_when(
      qc_flag == "unfilled" ~ "unfilled",
      is.na(Q_model)        ~ "out_of_range",
      TRUE                  ~ qc_flag
    )
  ) |>
  dplyr::select(
    station_id, site_id, location_id, rating_curve_period,
    timestamp, water_year, stage_cm, stage_source, qc_flag,
    Q_model, Max_CI, Min_CI, rating, rating_label, discharge_flag
  )

message("RC2 discharge NA count: ",
        sum(is.na(discharge_rc2$Q_model)), " of ", nrow(discharge_rc2))

write_csv(discharge_rc2, file.path(out_dir, "ssn703_discharge_RC2.csv"))
message("Saved: ssn703_discharge_RC2.csv")


# -----------------------------------------------------------------------------
# 4. RC3 discharge
# -----------------------------------------------------------------------------

message("\nProcessing RC3...")

stage_rc3 <- read_csv(
  file.path(per_rc, "ssn703_RC3_stage_qc.csv"),
  show_col_types = FALSE
)

message("RC3 rows: ", nrow(stage_rc3))
message("RC3 date range: ", min(stage_rc3$timestamp), " to ", max(stage_rc3$timestamp))

discharge_rc3 <- stage_rc3 |>
  mutate(
    stage_cm = stage_corrected * 100
  ) |>
  bind_cols(apply_rating(
    stage_cm = ifelse(stage_rc3$qc_flag == "unfilled", NA,
                      stage_rc3$stage_corrected * 100),
    rc = rc5
  )) |>
  mutate(
    rating        = 5L,
    rating_label  = "RC3 (Rating 5) -- loc_3, ssn703_d",
    stage_source  = "ssn703_d (loc_3)",
    discharge_flag = case_when(
      qc_flag == "unfilled" ~ "unfilled",
      is.na(Q_model)        ~ "out_of_range",
      TRUE                  ~ qc_flag
    )
  ) |>
  dplyr::select(
    station_id, site_id, location_id, rating_curve_period,
    timestamp, water_year, stage_cm, stage_source, qc_flag,
    Q_model, Max_CI, Min_CI, rating, rating_label, discharge_flag
  )

message("RC3 discharge NA count: ",
        sum(is.na(discharge_rc3$Q_model)), " of ", nrow(discharge_rc3))

write_csv(discharge_rc3, file.path(out_dir, "ssn703_discharge_RC3.csv"))
message("Saved: ssn703_discharge_RC3.csv")


# -----------------------------------------------------------------------------
# 5. Combined discharge timeseries
# -----------------------------------------------------------------------------

# Combined discharge timeseries
# RC3 filtered to 2023-09-15 -- this is the authoritative RC3 start date when
# the autosalt database was updated to pull stage from ssn703_d (loc_3).
# ssn703_d was physically installed 2021-09-02 and the RC3 stage QC file
# contains data from that date, but the database was still pulling stage from
# ssn703_c (loc_2) until 2023-09-15. Filtering RC3 here ensures one clean
# discharge value per timestamp with no overlap between RC2 and RC3.
discharge_combined <- bind_rows(
  discharge_rc2,
  discharge_rc3 |> filter(timestamp >= as.POSIXct("2023-09-15", tz = "UTC"))
) |>
  arrange(timestamp)

write_csv(discharge_combined,
          file.path(out_dir, "ssn703_discharge_combined.csv"))
message("\nSaved: ssn703_discharge_combined.csv")
message("Combined rows: ", nrow(discharge_combined))
message("Combined date range: ",
        min(discharge_combined$timestamp), " to ",
        max(discharge_combined$timestamp))


# -----------------------------------------------------------------------------
# 6. Summary check plot
# -----------------------------------------------------------------------------

p <- discharge_combined |>
  filter(!is.na(Q_model)) |>
  ggplot(aes(x = timestamp, y = Q_model)) +
  geom_line(data = ~ filter(., discharge_flag == "raw"),
            colour = "#333333", linewidth = 0.3, alpha = 0.7) +
  geom_point(data = ~ filter(., discharge_flag == "gf_spline"),
             colour = "#4DAF4A", size = 0.5) +
  geom_point(data = ~ filter(., discharge_flag == "gf_spline_event"),
             colour = "#FF7F00", size = 0.5) +
  theme_bw() +
  xlab("Date") + ylab("Discharge (m³/s)") +
  labs(title = "SSN703 -- discharge timeseries (RC2 + RC3)",
       subtitle = "NA values (unfilled gaps and out-of-range stage) not shown",
       caption = "Generated by 10_discharge.R") +
  theme(legend.position = "bottom")

out_path <- "02_processing/plots/ssn703_discharge_overview.pdf"
pdf(out_path, width = 12, height = 6)
print(p)
dev.off()
message("Saved: ", out_path)

message("\nDone. Next step: review discharge timeseries and proceed to validation.")
