# =============================================================================
# SSN703 Stage QC - Script 06: Gauging Table Preparation
# =============================================================================
# Purpose:
#   Prepare the gauging table for rating curve development by:
#   1. Assigning rating curve period (RC1, RC2, RC3) based on gauging date
#   2. Updating Old_New field for records after 2019
#   3. Applying +2cm datum offset to Stage_avg for gaugings in the ssn703_a era
#   4. Flagging gaugings with missing stage
#
# RC period boundaries (based on actual stage source for each gauging):
#   RC1: before 2018-09-14 20:00 -- stage from loc_1 (ssn703_a and ssn703_b)
#   RC2: 2018-09-14 20:00 to 2023-06-25 17:00 -- stage from loc_2 (ssn703_c)
#        NOTE: includes 2021-09-02 to 2023-06-25 when ssn703_d was physically
#        installed but autosalt db still pulled stage from ssn703_c -- stage
#        is valid and belongs to RC2 since it comes from loc_2
#   stage_suspect: 2023-06-25 17:00 to 2023-09-15 -- ssn703_c stage went bad
#        before db was updated to ssn703_d -- stage not trustworthy; flag and
#        exclude from curve fitting; retained in table for traceability
#   RC3: 2023-09-15 onward -- stage correctly from loc_3 (ssn703_d)
#
# Offset application:
#   ssn703_a era: before 2017-11-13 14:00 (ssn703_b install date)
#   Offset: +2cm added to Stage_avg to bring onto ssn703_b datum
#   Gaugings from 2017-11-13 onward: ssn703_b is authoritative -- no offset
#
# Stage units:
#   Stage_avg in this table is in cm -- kept as-is for rating curve work
#   Offset applied in cm (+2cm)
#   Stage on sensor network is in m -- do not mix units
#
# Inputs:
#   03_docs/metadata/ssn703_gaugings_raw.csv  -- the gauging table as provided
#
# Outputs:
#   03_docs/metadata/ssn703_gaugings_prepped.csv  -- gauging table ready for RC work
#
# Flags:
#   stage_source   -- "raw", "offset_applied" (ssn703_a era corrected), "no_stage"
#   stage_status   -- "ok", "stage_missing" (no Stage_avg),
#                     "stage_suspect" (ssn703_c bad data period 2023-06-25 to 2023-09-15)
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)


# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

meta_dir <- "03_docs/metadata"

# Key datetime boundaries -- all PST
SSN703_B_INSTALL  <- as.POSIXct("2017-11-13 14:00:00", tz = "Etc/GMT+8")  # offset applies before this
RC1_END           <- as.POSIXct("2018-09-14 20:00:00", tz = "Etc/GMT+8")  # ssn703_c install -- RC2 starts
RC2_END           <- as.POSIXct("2023-06-25 17:00:00", tz = "Etc/GMT+8")  # ssn703_c stage goes bad -- RC2 ends
SUSPECT_END       <- as.POSIXct("2023-09-15 00:00:00", tz = "Etc/GMT+8")  # autosalt db updated to ssn703_d -- RC3 starts

OFFSET_CM <- 2  # +2cm applied to ssn703_a era gaugings


# -----------------------------------------------------------------------------
# 1. Load gauging table
# -----------------------------------------------------------------------------

gaugings <- read_csv(
  file.path(meta_dir, "ssn703_gaugings_raw.csv"),
  show_col_types = FALSE
)

message("Loaded ", nrow(gaugings), " gaugings")


# -----------------------------------------------------------------------------
# 2. Parse datetime
# -----------------------------------------------------------------------------
# Date and Start_time are separate columns -- combine into a single datetime

gaugings <- gaugings |>
  mutate(
    datetime = mdy_hms(paste(Date, Start_time), tz = "Etc/GMT+8")
  )

# Check for parse failures
n_failed <- sum(is.na(gaugings$datetime))
if (n_failed > 0) {
  warning(n_failed, " rows failed datetime parsing -- review Date/Start_time columns")
  gaugings |> filter(is.na(datetime)) |> select(Date, Start_time) |> print()
}


# -----------------------------------------------------------------------------
# 3. Assign rating curve period
# -----------------------------------------------------------------------------
# RC period is based on the actual stage source for each gauging.
# Key principle: stage from ssn703_c (loc_2) belongs to RC2 regardless of
# whether ssn703_d was physically installed at the time -- the autosalt db
# continued pulling stage from ssn703_c until 2023-09-15, so those gaugings
# carry loc_2 stage and belong to RC2.
#
# The only window requiring a suspect flag is 2023-06-25 to 2023-09-15 where
# ssn703_c stage was genuinely bad before the db was updated to ssn703_d.

gaugings <- gaugings |>
  mutate(
    rating_curve_period = case_when(
      datetime < RC1_END     ~ "RC1",
      datetime < RC2_END     ~ "RC2",
      datetime < SUSPECT_END ~ "stage_suspect",
      TRUE                   ~ "RC3"
    )
  )

message("\nGaugings per RC period:")
gaugings |> count(rating_curve_period) |> print()


# -----------------------------------------------------------------------------
# 4. Update Old_New
# -----------------------------------------------------------------------------
# Old_New was last updated in 2019. Update for all records:
#   Old = RC1 (loc_1, original rating curve location)
#   New = RC2 and RC3 (new locations, new curves)
# This overwrites the existing column with a consistent definition.

gaugings <- gaugings |>
  mutate(
    Old_New = case_when(
      rating_curve_period == "RC1" ~ "Old",
      TRUE                         ~ "New"
    )
  )


# -----------------------------------------------------------------------------
# 5. Apply datum offset to ssn703_a era gaugings
# -----------------------------------------------------------------------------
# Gaugings before ssn703_b install (2017-11-13 14:00) used ssn703_a for stage.
# ssn703_a reads ~2cm lower than ssn703_b -- add +2cm to bring onto ssn703_b datum.
# Gaugings from ssn703_b install onward: no offset needed.
# Gaugings with no Stage_avg: offset not applied (nothing to correct).

gaugings <- gaugings |>
  mutate(
    in_703a_era   = datetime < SSN703_B_INSTALL,
    stage_source  = case_when(
      is.na(Stage_avg)  ~ "no_stage",
      in_703a_era       ~ "offset_applied",
      TRUE              ~ "raw"
    ),
    Stage_avg_corrected = case_when(
      in_703a_era & !is.na(Stage_avg) ~ Stage_avg + OFFSET_CM,
      TRUE                             ~ Stage_avg
    )
  ) |>
  select(-in_703a_era)

message("\nOffset application summary:")
gaugings |> count(stage_source) |> print()

message("\nGaugings with offset applied: ",
        sum(gaugings$stage_source == "offset_applied"),
        " (pre 2017-11-13, ssn703_a era, +", OFFSET_CM, "cm applied)")


# -----------------------------------------------------------------------------
# 6. Flag stage status
# -----------------------------------------------------------------------------

gaugings <- gaugings |>
  mutate(
    stage_status = case_when(
      is.na(Stage_avg)                       ~ "stage_missing",
      rating_curve_period == "stage_suspect" ~ "stage_suspect",
      TRUE                                   ~ "ok"
    )
  )

# Note: stage_suspect gaugings are retained in the output table for
# traceability but should be excluded from curve fitting. Stage values
# in this window come from ssn703_c after its stage record went bad.

message("\nStage status:")
gaugings |> count(stage_status) |> print()

message("\nStage missing by RC period:")
gaugings |>
  filter(stage_status == "stage_missing") |>
  count(rating_curve_period) |>
  print()


# -----------------------------------------------------------------------------
# 7. Reorder and save
# -----------------------------------------------------------------------------

gaugings_out <- gaugings |>
  select(
    # identifiers
    EventID, MID, SiteID, Method,
    # datetime
    datetime, Date, Start_time, WY,
    # RC assignment
    rating_curve_period, Old_New, Final_rating_curve,
    # stage
    Stage_avg, Stage_avg_corrected, Stage_stdv, Stage_delta,
    stage_source, stage_status,
    # discharge
    Q_meas, Q_rel_unc,
    # other
    Mixing, ecb, Comments
  ) |>
  arrange(datetime)

write_csv(gaugings_out, file.path(meta_dir, "ssn703_gaugings_prepped.csv"))

message("\nSaved: ", file.path(meta_dir, "ssn703_gaugings_prepped.csv"))


# -----------------------------------------------------------------------------
# 8. Summary check plot
# -----------------------------------------------------------------------------
# Stage vs discharge coloured by RC period -- first look at the data
# before any curve fitting. Excludes gaugings with missing stage.

p <- gaugings_out |>
  filter(stage_status == "ok") |>
  ggplot(aes(x = Stage_avg_corrected, y = Q_meas,
             colour = rating_curve_period, shape = Method)) +
  geom_point(
    data = gaugings_out |> filter(stage_status == "stage_suspect"),
    aes(x = Stage_avg_corrected, y = Q_meas),
    colour = "grey70", shape = 4, size = 2, inherit.aes = FALSE,
    na.rm = TRUE
  ) +
  geom_point(size = 2, alpha = 0.8, na.rm = TRUE) +
  scale_colour_manual(values = c(
    "RC1" = "#E41A1C",
    "RC2" = "#4DAF4A",
    "RC3" = "#984EA3"
  )) +
  labs(
    title    = "SSN703 -- Stage vs discharge by rating curve period",
    subtitle = paste0("All gaugings with stage | Stage in cm | offset of +",
                      OFFSET_CM, "cm applied to ssn703_a era gaugings\n",
                      "Grey crosses = stage_suspect (ssn703_c bad data period -- excluded from curve fitting)"),
    x        = "Stage corrected (cm)",
    y        = "Discharge (m³/s)",
    colour   = "RC period",
    shape    = "Method",
    caption  = "Plot generated by 06_gauging_table_prep.R"
  ) +
  theme_bw() +
  theme(
    legend.position  = "bottom",
    panel.grid.minor = element_blank(),
    plot.caption     = element_text(hjust = 0, size = 8, colour = "grey40")
  )

out_path <- "02_processing/plots/ssn703_gauging_overview.pdf"
pdf(out_path, width = 10, height = 7)
print(p)
dev.off()

message("Saved: ", out_path)
message("\nNext step: proceed to rating curve development using ssn703_gaugings_prepped.csv")
message("  Filter on stage_status == 'ok' to exclude gaugings with no stage")
message("  Filter on Final_rating_curve == 'Y' for curve fitting")
message("  Use Stage_avg_corrected as the stage value throughout")