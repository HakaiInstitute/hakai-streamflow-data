# =============================================================================
# SSN703 Stage QC - Script 01: Load and Parse Raw Stage Data
# =============================================================================
# Purpose:
#   Read all raw sensor files for SSN703, standardize column names,
#   assign site IDs from file names, and produce a single tidy dataframe
#   ready for QC and offset correction.
#
# Inputs:
#   data/01_raw/ssn703_a.csv
#   data/01_raw/ssn703_b.csv
#   data/01_raw/ssn703_c.csv
#   data/01_raw/ssn703_d.csv
#
# Outputs:
#   data/02_parsed/ssn703_all_raw.rds  -- all sensors combined, tidy format
#
# Notes:
#   - File name is the authoritative site ID; internal column names are ignored
#   - All files share a 4-row non-standard header (units, site, variable type)
#     which is skipped on read; columns are renamed to standard names
#   - Timestamps are in PST; no daylight saving adjustment needed
#   - stage_avg is the primary stage value used downstream
#   - Sensor locations and roles are joined from sensor_registry.csv after load
#   - All downstream scripts receive station_id, location_id, sensor_role,
#     rating_curve_period, date_start, date_end, bad_data_start, bad_data_end
#     via this join
#
# Author: [your name]
# Date: [date]
# =============================================================================


library(tidyverse)
library(lubridate)


# -----------------------------------------------------------------------------
# 1. Define file paths and site IDs
# -----------------------------------------------------------------------------
# Site IDs are derived from file names -- this is the authoritative identifier.
# If files are added or renamed, this is the only place that needs updating.

raw_dir <- "01_raw/SSN703"

sensor_files <- tibble(
  file_path = list.files(raw_dir, pattern = "ssn703.*\\.csv$", full.names = TRUE)
) |>
  mutate(
    site_id = tools::file_path_sans_ext(basename(file_path))  # e.g. "ssn703_a"
  )

# Confirm files found -- stop early if something is missing
message("Files found:")
walk(sensor_files$file_path, ~ message("  ", .x))

if (nrow(sensor_files) == 0) stop("No SSN703 files found in ", raw_dir)


# -----------------------------------------------------------------------------
# 2. Define standardized column names
# -----------------------------------------------------------------------------
# Raw files have 4 header rows. Row 1 is the true header but contains
# site-specific names we don't want to carry forward. We skip all 4 header
# rows and assign standard names on read, making all files interchangeable
# downstream.
#
# Standard names:
#   timestamp   -- measurement time (PST)
#   year        -- calendar year
#   month       -- calendar month
#   water_year  -- hydrological water year (e.g. "2013-2014")
#   stage_inst  -- instantaneous stage reading (m)
#   stage_avg   -- 5-min average stage (m) -- PRIMARY VALUE
#   stage_min   -- 5-min minimum stage (m)
#   stage_max   -- 5-min maximum stage (m)
#   stage_sd    -- 5-min standard deviation of stage (m)

std_col_names <- c(
  "timestamp", "year", "month", "water_year",
  "stage_inst", "stage_avg", "stage_min", "stage_max", "stage_sd"
)


# -----------------------------------------------------------------------------
# 3. Read and parse all files
# -----------------------------------------------------------------------------

read_sensor_file <- function(file_path, site_id) {
  
  message("Reading: ", site_id)
  
  raw <- read_csv(
    file_path,
    skip      = 4,           # skip the 4 non-standard header rows
    col_names = std_col_names,
    col_types = cols(
      timestamp  = col_character(),  # parse manually below for control
      year       = col_integer(),
      month      = col_character(),
      water_year = col_character(),
      stage_inst = col_double(),
      stage_avg  = col_double(),
      stage_min  = col_double(),
      stage_max  = col_double(),
      stage_sd   = col_double()
    ),
    na = c("", "NA", "NaN")
  ) |>
    mutate(
      site_id   = site_id,
      timestamp = ymd_hms(timestamp, tz = "Etc/GMT+8")  # PST = UTC-8, no DST
    ) |>
    # site_id and timestamp to the front for readability
    select(site_id, timestamp, year, month, water_year,
           stage_inst, stage_avg, stage_min, stage_max, stage_sd)
  
  return(raw)
}


# Read all files and bind into one dataframe
ssn703_raw <- sensor_files |>
  pmap(~ read_sensor_file(..1, ..2)) |>
  bind_rows()


# -----------------------------------------------------------------------------
# 4. Join sensor registry metadata
# -----------------------------------------------------------------------------
# Join station_id, location_id, sensor_role, rating_curve_period, deployment
# dates, and bad data period dates from sensor_registry. This means all
# downstream scripts receive full context without needing to read metadata
# separately.
#
# Join key: site_id (data) = sensor_id (registry)
# All metadata columns travel with the data from this point forward.

sensor_registry <- read_csv(
  "03_docs/metadata/sensor_registry.csv",
  show_col_types = FALSE
) |>
  select(
    site_id        = sensor_id,  # rename to match data column
    station_id,
    location_id,
    sensor_role,
    rating_curve_period,
    date_start,                  # deployment start -- used to trim data in 04
    date_end,                    # deployment end -- NA = ongoing
    bad_data_start,
    bad_data_end
  )

# Check all site_ids have a match in the registry before joining
unmatched <- anti_join(ssn703_raw |> distinct(site_id), sensor_registry, by = "site_id")
if (nrow(unmatched) > 0) {
  warning("The following site_ids have no match in sensor_registry -- check file names and registry:\n",
          paste(unmatched$site_id, collapse = ", "))
}

ssn703_raw <- ssn703_raw |>
  left_join(sensor_registry, by = "site_id") |>
  # reorder columns: identifiers first, then timestamps, then stage values, then metadata
  select(
    station_id, site_id, location_id, sensor_role, rating_curve_period,
    date_start, date_end, bad_data_start, bad_data_end,
    timestamp, year, month, water_year,
    stage_inst, stage_avg, stage_min, stage_max, stage_sd
  )


# -----------------------------------------------------------------------------
# 5. Basic checks
# -----------------------------------------------------------------------------

message("\n--- Basic checks ---")

# Row counts per sensor
ssn703_raw |>
  count(site_id) |>
  mutate(label = paste0(site_id, ": ", n, " rows")) |>
  pull(label) |>
  walk(message)

# Date ranges per sensor
ssn703_raw |>
  group_by(site_id) |>
  summarise(
    start         = min(timestamp, na.rm = TRUE),
    end           = max(timestamp, na.rm = TRUE),
    n_missing_avg = sum(is.na(stage_avg))
  ) |>
  print()

# Flag if any timestamps are duplicated within a sensor
dups <- ssn703_raw |>
  group_by(site_id, timestamp) |>
  filter(n() > 1)

if (nrow(dups) > 0) {
  warning("Duplicate timestamps found -- review before proceeding:")
  print(dups)
} else {
  message("No duplicate timestamps found.")
}


# -----------------------------------------------------------------------------
# 6. Save parsed output
# -----------------------------------------------------------------------------

out_dir <- "02_processing/data_parsed"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

saveRDS(ssn703_raw, file.path(out_dir, "ssn703_all_raw.rds"))

message("\nSaved: ", file.path(out_dir, "ssn703_all_raw.rds"))
message("Done -- proceed to 02_inspect_stage.R")