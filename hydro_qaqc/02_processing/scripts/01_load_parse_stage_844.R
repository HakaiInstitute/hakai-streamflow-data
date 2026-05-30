# =============================================================================
# SSN844 Stage QC - Script 01: Load and Parse Raw Stage Data
# =============================================================================
# Purpose:
#   Read raw sensor files for SSN844, standardize column names, assign site
#   IDs from file names, and produce a single tidy dataframe ready for QC.
#
# Inputs:
#   01_raw/SSN844/ssn844.csv       -- 9-column format (standard)
#   01_raw/SSN844/ssn844_sa.csv    -- 11-column format (extra QC flag columns)
#
# Outputs:
#   02_processing/data_parsed/ssn844_all_raw.rds
#
# Notes:
#   - ssn844    = primary sensor, loc_1, 2014-present, single location throughout
#   - ssn844_sa = supplementary sensor, loc_2, gap filling only
#   - ssn844_sa has a different column structure (11 cols vs 9) -- handled below
#   - No datum corrections required (single location, no primary sensor overlaps)
#   - No offset calculation needed -- proceed directly to 02_inspect then 04_qc
#   - File name is the authoritative site ID
#   - All files share a 4-row non-standard header; skipped on read
#   - Timestamps are PST (Etc/GMT+8); no daylight saving adjustment needed
#   - stage_avg is the primary stage value used downstream
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)

# -----------------------------------------------------------------------------
# 1. Define file paths and site IDs
# -----------------------------------------------------------------------------

raw_dir <- "01_raw/SSN844"

sensor_files <- tibble(
  file_path = list.files(raw_dir, pattern = "ssn844.*\\.csv$", full.names = TRUE)
) |>
  mutate(
    site_id = tools::file_path_sans_ext(basename(file_path))
  )

message("Files found:")
walk(sensor_files$file_path, ~ message("  ", .x))

if (nrow(sensor_files) == 0) stop("No SSN844 files found in ", raw_dir)

# -----------------------------------------------------------------------------
# 2. Standardized column names
# -----------------------------------------------------------------------------
# ssn844 (primary): 9 columns -- standard format
# ssn844_sa (supplementary): 11 columns -- two extra QC flag columns after water_year
# Both produce the same output columns after parsing; SA extras are dropped

std_col_names_primary <- c(
  "timestamp", "year", "month", "water_year",
  "stage_inst", "stage_avg", "stage_min", "stage_max", "stage_sd"
)

std_col_names_sa <- c(
  "timestamp", "year", "month", "water_year",
  "stage_ql", "stage_qf", "stage_uql",
  "stage_med", "stage_avg", "stage_min", "stage_max"
)

# -----------------------------------------------------------------------------
# 3. Read and parse all files
# -----------------------------------------------------------------------------

read_sensor_file <- function(file_path, site_id) {

  message("Reading: ", site_id)

  is_sa   <- site_id == "ssn844_sa"
  col_nms <- if (is_sa) std_col_names_sa else std_col_names_primary

  raw <- read_csv(
    file_path,
    skip      = 4,
    col_names = col_nms,
    col_types = cols(.default = col_character()),
    na        = c("", "NA", "NaN")
  ) |>
    mutate(
      site_id   = site_id,
      timestamp = ymd_hms(timestamp, tz = "Etc/GMT+8"),
      stage_avg = as.numeric(stage_avg),
      stage_min = as.numeric(stage_min),
      stage_max = as.numeric(stage_max)
    )

  # For primary sensor, also parse stage_inst and stage_sd
  if (!is_sa) {
    raw <- raw |>
      mutate(
        stage_inst = as.numeric(stage_inst),
        stage_sd   = as.numeric(stage_sd)
      )
  } else {
    # SA file has no stage_inst or stage_sd -- add as NA for consistent binding
    raw <- raw |>
      mutate(
        stage_inst = NA_real_,
        stage_sd   = NA_real_
      )
  }

  raw |>
    select(site_id, timestamp, year, month, water_year,
           stage_inst, stage_avg, stage_min, stage_max, stage_sd)
}

ssn844_raw <- sensor_files |>
  pmap(~ read_sensor_file(..1, ..2)) |>
  bind_rows()

# -----------------------------------------------------------------------------
# 4. Join sensor registry metadata
# -----------------------------------------------------------------------------

sensor_registry <- read_csv(
  "03_docs/metadata/sensor_registry.csv",
  show_col_types = FALSE
) |>
  filter(station_id == "SSN844") |>
  select(
    site_id        = sensor_id,
    station_id,
    location_id,
    sensor_role,
    rating_curve_period,
    date_start,
    date_end,
    bad_data_start,
    bad_data_end
  )

unmatched <- anti_join(ssn844_raw |> distinct(site_id), sensor_registry, by = "site_id")
if (nrow(unmatched) > 0) {
  warning("The following site_ids have no match in sensor_registry:\n",
          paste(unmatched$site_id, collapse = ", "))
}

ssn844_raw <- ssn844_raw |>
  left_join(sensor_registry, by = "site_id") |>
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

ssn844_raw |>
  count(site_id) |>
  mutate(label = paste0(site_id, ": ", n, " rows")) |>
  pull(label) |>
  walk(message)

ssn844_raw |>
  group_by(site_id) |>
  summarise(
    start         = min(timestamp, na.rm = TRUE),
    end           = max(timestamp, na.rm = TRUE),
    n_missing_avg = sum(is.na(stage_avg)),
    .groups       = "drop"
  ) |>
  print()

# Confirm stage_avg is non-NA for both sensors
message("\nstage_avg non-NA counts:")
ssn844_raw |>
  group_by(site_id) |>
  summarise(
    n_total    = n(),
    n_valid    = sum(!is.na(stage_avg)),
    pct_valid  = round(sum(!is.na(stage_avg)) / n() * 100, 1),
    .groups    = "drop"
  ) |>
  print()

dups <- ssn844_raw |>
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

saveRDS(ssn844_raw, file.path(out_dir, "ssn844_all_raw.rds"))

message("\nSaved: ", file.path(out_dir, "ssn844_all_raw.rds"))
message("Done -- proceed to 02_inspect_stage_844.R")
