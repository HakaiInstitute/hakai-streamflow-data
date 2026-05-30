# =============================================================================
# SSN703 Stage QC - Script 00: Build Metadata Tables
# =============================================================================
# Purpose:
#   Generate two clean metadata CSVs that drive all downstream QC decisions:
#
#   1. sensor_registry.csv   -- one row per sensor; stable deployment facts
#   2. overlap_registry.csv  -- one row per overlap pair; QC decisions
#
#   These are the authoritative source of truth for sensor history and
#   overlap handling. Edit these tables (not the code) as new information
#   becomes available (e.g. confirmed bad data dates, refined clean windows).
#
# Outputs:
#   03_docs/metadata/sensor_registry.csv
#   03_docs/metadata/overlap_registry.csv
#
# Note on offset values:
#   Offset values and uncertainty are NOT stored here -- they are outputs of
#   the offset calculation script and written to:
#   03_docs/metadata/offsets.csv
#   Do not add offset columns to overlap_registry manually.
#
# Notes:
#   - "ongoing" date_end values are left as NA -- interpreted as Sys.time() downstream
#   - Placeholders marked as NA where dates are not yet confirmed
#   - bad_data_start/end refer to the period where the sensor is clearly
#     degraded and should not be used -- does not imply data before is perfect
#   - Tables link on station_id + sensor_id
#   - All timestamps are PST
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)

out_dir <- "03_docs/metadata"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)


# =============================================================================
# TABLE 1: sensor_registry
# =============================================================================
# One row per sensor. Captures stable deployment facts and known bad data
# periods. Does not capture overlap decisions -- those are in overlap_registry.
#
# Columns:
#   station_id          -- station identifier (e.g. SSN703)
#   sensor_id           -- unique sensor identifier (e.g. ssn703_a)
#   location_id         -- physical location of sensor (loc_1, loc_2, loc_3...)
#                          sensors at the same location share a rating curve period
#   sensor_role         -- primary or supplementary
#   date_start          -- sensor deployment datetime (PST)
#   date_end            -- sensor removal datetime (PST); NA = ongoing
#   bad_data_start      -- datetime where sensor degradation is confirmed (PST)
#                          NA = no confirmed bad period or not yet assessed
#   bad_data_end        -- end of confirmed bad period; usually same as date_end
#                          NA = not yet confirmed
#   rating_curve_period -- which rating curve this sensor contributes to
#                          sensors at different locations get different periods
#                          supplementary sensors: NA
#   notes               -- any additional context
#
# Key principle for SSN703:
#   ssn703_a and ssn703_b are at the same location (loc_1) -- one rating curve
#   ssn703_b is the authoritative sensor from its install date onward
#   ssn703_c is a new location (loc_2) -- authoritative from install, new curve
#   ssn703_d is a new location (loc_3) -- authoritative from install, new curve
# =============================================================================

sensor_registry <- tribble(
  ~station_id, ~sensor_id,   ~location_id, ~sensor_role,    ~date_start,            ~date_end,              ~bad_data_start,        ~bad_data_end,          ~rating_curve_period, ~notes,
  
  # --- SSN626 ---
  "SSN626",    "ssn626",     "loc_1",      "primary",       "2014-08-02 11:55:00",  NA,                     NA,                     NA,                     "RC1",                "single sensor; no overlaps",
  "SSN626",    "ssn626_sa",  "loc_2",      "supplementary", "2018-08-11 23:00:00",  "2024-05-10 13:00:00",  NA,                     NA,                     NA,                   "upstream supplementary; gaps present; use for gap filling where relationship sufficient",
  
  # --- SSN703 ---
  # loc_1: ssn703_a and ssn703_b are the same-ish location -- one rating curve period
  #        ssn703_b is authoritative from its install date onward
  # loc_2: ssn703_c is a different location -- separate rating curve period; authoritative from install
  # loc_3: ssn703_d is a different location again -- separate rating curve period; authoritative from install
  # loc_4: ssn703_sa is upstream supplementary; independent of loc_1/2/3
  "SSN703",    "ssn703_a",   "loc_1",      "primary",       "2014-08-03 10:55:00",  "2018-09-12 01:00:00",  NA,                     "2018-09-12 01:00:00",  "RC1",                "original sensor; failed during ssn703_b overlap; bad_data_start TBC from visual assessment; offset of +0.02m applied to bring onto ssn703_b datum",
  "SSN703",    "ssn703_b",   "loc_1",      "primary",       "2017-11-13 14:00:00",  "2021-03-25 10:35:00",  NA,                     NA,                     "RC1",                "installed to replace ssn703_a; close to loc_1 -- same rating curve period; authoritative from install date; stage record clean through to date_end -- temperature failed 2019-05-28 but stage unaffected; later failed vs ssn703_c",
  "SSN703",    "ssn703_c",   "loc_2",      "primary",       "2018-09-14 20:00:00",  "2023-08-03 07:00:00",  "2023-06-25 17:00:00",  "2023-08-03 07:00:00",  "RC2",                "new location; separate rating curve period; authoritative from install date; stage went bad 2023-06-25 17:00 -- temperature bad_data_start was 2021-03-05 but stage record clean until June 2023; later failed vs ssn703_d",
  "SSN703",    "ssn703_d",   "loc_3",      "primary",       "2021-09-02 12:55:00",  NA,                     "2023-10-23 00:00:00",  "2023-11-26 23:55:00",  "RC3",                "new location; third distinct rating curve period; authoritative from install date; ongoing; known bad data period Oct-Nov 2023",
  "SSN703",    "ssn703_sa",  "loc_4",      "supplementary", "2018-08-08 23:00:00",  "2025-05-05 10:00:00",  NA,                     NA,                     NA,                   "upstream supplementary; independent of loc_1/2/3 rating curve locations; gaps present",
  
  # --- SSN844 ---
  "SSN844",    "ssn844",     "loc_1",      "primary",       "2014-09-10 16:30:00",  NA,                     NA,                     NA,                     "RC1",                "single sensor; no overlaps",
  "SSN844",    "ssn844_sa",  "loc_2",      "supplementary", "2018-09-02 00:00:00",  "2024-05-09 09:00:00",  NA,                     NA,                     NA,                   "upstream supplementary; gaps present; use for gap filling where relationship sufficient",
  
  # --- SSN1015 ---
  "SSN1015",   "ssn1015_a",  "loc_1",      "primary",       "2014-07-31 13:50:00",  "2025-09-05 09:35:00",  "2019-09-09 11:00:00",  "2025-09-05 09:35:00",  "RC1",                "left in inadvertently after ssn1015_b installed; data after bad_data_start not used",
  "SSN1015",   "ssn1015_b",  "loc_1",      "primary",       "2019-09-09 11:00:00",  NA,                     NA,                     NA,                     "RC1",                "primary from install date onward",
  "SSN1015",   "ssn1015_sa", "loc_2",      "supplementary", "2018-09-05 00:00:00",  "2025-05-07 09:00:00",  NA,                     NA,                     NA,                   "upstream supplementary; gaps present; use for gap filling where relationship sufficient"
  
) |>
  mutate(across(c(date_start, date_end, bad_data_start, bad_data_end),
                ~ as.POSIXct(.x, tz = "Etc/GMT+8")))


# =============================================================================
# TABLE 2: overlap_registry
# =============================================================================
# One row per overlap pair. Captures QC decisions about how each overlap
# should be handled. Links to sensor_registry on station_id + sensor_id.
#
# Columns:
#   station_id            -- station identifier
#   sensor_id_failing     -- the sensor that was degrading / being replaced
#   sensor_id_replacement -- the new sensor installed during the overlap
#   overlap_start         -- when both sensors were running simultaneously
#   overlap_end           -- end of simultaneous period
#   clean_window_start    -- start of period suitable for offset calculation
#                           NA if offset_method = storm_peaks or none
#   clean_window_end      -- end of period suitable for offset calculation
#                           NA if offset_method = storm_peaks or none
#   stage_threshold_m     -- minimum stage (m) for offset calculation
#                           NA if no threshold applied
#   offset_use            -- yes: compute datum offset
#                           no: location break, no offset computed
#   offset_method         -- how offset is calculated:
#                           "storm_peaks"    use storm events across overlap
#                           "install_window" use stable period after install
#                           "none"           location break, no offset
#   notes                 -- reasoning for decisions
#
# NOTE: offset_value_m and offset_uncertainty_m are NOT columns here.
#       They are outputs of the offset calculation script -- see offsets.csv
# =============================================================================

overlap_registry <- tribble(
  ~station_id, ~sensor_id_failing, ~sensor_id_replacement, ~overlap_start,         ~overlap_end,           ~clean_window_start, ~clean_window_end, ~stage_threshold_m, ~offset_use, ~offset_method, ~notes,
  
  # SSN703: ssn703_a (failing) vs ssn703_b (replacement)
  # Same location (loc_1) -- datum offset calculated and applied (+0.02m to 703a)
  # Decoupling is seasonal and intermittent -- not a hard breakpoint
  # Storm peaks used across full overlap; offset confirmed from offset vs stage plot
  "SSN703",    "ssn703_a",         "ssn703_b",             "2017-11-13 14:00:00",  "2018-09-12 01:00:00",  NA,                  NA,                NA,                 "yes",       "storm_peaks",  "decoupling is seasonal and intermittent -- no hard clean window end; storm peaks used across full overlap; offset = +0.02m applied to ssn703_a; uncertainty +/-0.01m; no formal survey",
  
  # SSN703: ssn703_b (failing) vs ssn703_c (replacement)
  # Different location (loc_1 -> loc_2) -- genuinely different water surface
  # No offset calculated -- ssn703_c is authoritative from install date
  "SSN703",    "ssn703_b",         "ssn703_c",             "2018-09-14 20:00:00",  "2021-03-25 10:35:00",  NA,                  NA,                NA,                 "no",        "none",         "location change -- loc_1 to loc_2; sensors not on same water surface; no datum offset computed; ssn703_c authoritative from install date; treat as start of RC2",
  
  # SSN703: ssn703_c (failing) vs ssn703_d (replacement)
  # Different location (loc_2 -> loc_3) -- different hydraulic environment
  # No offset calculated -- ssn703_d is authoritative from install date
  "SSN703",    "ssn703_c",         "ssn703_d",             "2021-09-02 12:55:00",  "2023-08-03 07:00:00",  NA,                  NA,                NA,                 "no",        "none",         "location change -- loc_2 to loc_3; different hydraulic geometry and likely different control; no datum offset computed; ssn703_d authoritative from install date; treat as start of RC3",
  
  # SSN1015: ssn1015_a (left in) vs ssn1015_b (replacement)
  # Same location -- ssn1015_a data after install of ssn1015_b not used
  # No offset needed -- ssn1015_b is authoritative from install onward
  "SSN1015",   "ssn1015_a",        "ssn1015_b",            "2019-09-09 11:00:00",  "2025-09-05 09:35:00",  NA,                  NA,                NA,                 "no",        "none",         "ssn1015_a left in inadvertently; data after 2019-09-09 11:00 not used; ssn1015_b authoritative from install date"
  
) |>
  mutate(across(c(overlap_start, overlap_end, clean_window_start, clean_window_end),
                ~ as.POSIXct(.x, tz = "Etc/GMT+8")))


# =============================================================================
# Save outputs
# =============================================================================

write_csv(sensor_registry,  file.path(out_dir, "sensor_registry.csv"))
write_csv(overlap_registry, file.path(out_dir, "overlap_registry.csv"))

message("Saved: ", file.path(out_dir, "sensor_registry.csv"))
message("Saved: ", file.path(out_dir, "overlap_registry.csv"))
message("Done -- review CSVs before proceeding to 01_load_stage_data.R")
message("\nReminder -- fields still to confirm:")
message("  ssn703_a : bad_data_start (currently NA -- TBC from visual assessment)")