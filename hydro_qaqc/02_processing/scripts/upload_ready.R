# ==============================================================================
# format_for_network_upload.R
#
# Converts a single sensor/site QC output file (04_outputs/per_sensor) into the
# Hakai Telemetry Network's expected upload format, joining back the raw
# Avg/Min/Max/Std columns from 01_raw.
#
# Run explicitly per sensor/site -- no internal sensor lookup table. Set the
# parameters in the block below for each run.
# ==============================================================================

library(readr)
library(dplyr)
library(lubridate)
library(stringr)

# ------------------------------------------------------------------------------
# 1. PARAMETERS -- set these explicitly for each run
# ------------------------------------------------------------------------------

# Sensor generation number and short code (PLS/PLS2/PLS3/PLS4, etc.)
# e.g. PT4 at ssn703_d -> sensor_gen = "4", sensor_code = "PLS4"
sensor_gen  <- "3"
sensor_code <- "PLS3"

# Network site code used in the header (e.g. "SSN703US")
site_code <- "SSN703US"

# Path to the QC pipeline's per-sensor stage output (source of stage_corrected,
# qc_flag, offset_applied, etc.)
qc_output_path <- "C:/Users/Emily/Documents/git-repos/hydro_qaqc/04_outputs/per_sensor/ssn703_ssn703_c_stage_qc.csv"

# Path to the corresponding raw file (source of the original _Avg/_Min/_Max/_Std
# burst statistics). Set explicitly per run -- naming pattern in 01_raw may vary
# by site, so no automatic construction is attempted here.
raw_input_path <- "C:/Users/Emily/Documents/git-repos/hydro_qaqc/01_raw/SSN703/ssn703_c.csv"

# Output path for the upload-ready file
output_path <- "C:/Users/Emily/Documents/git-repos/hydro_qaqc/04_outputs/SSN703_PLS3_upload_ready.csv"

# ------------------------------------------------------------------------------
# 2. LOAD SOURCE DATA
# ------------------------------------------------------------------------------

qc_data <- read_csv(
  qc_output_path,
  col_types = cols(
    timestamp = col_datetime(format = "%Y-%m-%dT%H:%M:%SZ"),
    .default  = col_guess()
  )
)

# Raw files carry the same 4-row network header block, and their timestamp
# ("Measurement time") is already in PST -- NOT UTC like the QC output's
# `timestamp` column. Skip the header rows and parse accordingly.
raw_data <- read_csv(
  raw_input_path,
  skip = 4,
  col_names = c("measurement_time_pst_raw", "Year_raw", "Month_raw", "WaterYear_raw",
                "raw_value", "Avg", "Min", "Max", "Std"),
  # NOTE: this assumes the raw file's 9-column layout matches the original
  # example header exactly: time, Year, Month, WaterYear, raw value, Avg,
  # Min, Max, Std. Adjust if the actual file differs once confirmed.
  # Month_raw arrives as an abbreviated name (e.g. "Aug"), not numeric --
  # read as character, not used downstream (Year/Month/WaterYear for the
  # output are derived fresh from the QC data's own timestamp instead).
  col_types = cols(
    measurement_time_pst_raw = col_datetime(format = "%Y-%m-%d %H:%M:%S"),
    Month_raw = col_character(),
    .default = col_guess()
  ),
  na = ""
)

# ------------------------------------------------------------------------------
# 3. JOIN raw Avg/Min/Max/Std back onto the QC output by timestamp
#    QC output's `timestamp` is UTC -- convert to PST first so it lines up
#    with the raw file's PST-native timestamps.
# ------------------------------------------------------------------------------

qc_data <- qc_data %>%
  mutate(measurement_time_pst = with_tz(timestamp, tzone = "Etc/GMT+8"))

joined <- qc_data %>%
  left_join(raw_data, by = c("measurement_time_pst" = "measurement_time_pst_raw"))

# ------------------------------------------------------------------------------
# 4. APPLY DATUM OFFSET to Avg/Min/Max (NOT Std -- offset is a constant shift
#    and does not change spread)
# ------------------------------------------------------------------------------

joined <- joined %>%
  mutate(
    stat_avg_corrected = Avg + offset_applied,
    stat_min_corrected = Min + offset_applied,
    stat_max_corrected = Max + offset_applied,
    stat_std_corrected = Std
  )
# NOTE: replace Avg/Min/Max/Std above with the actual raw-file column names
# once confirmed.

# ------------------------------------------------------------------------------
# 5. DERIVE Q_level, Q_flags, UNESCO_Q_level FROM qc_flag
# ------------------------------------------------------------------------------

joined <- joined %>%
  mutate(
    Q_level = case_when(
      qc_flag == "raw"             ~ 2,
      qc_flag %in% c("gf_sa", "gf_spline", "gf_spline_event", "unfilled") ~ 3,
      TRUE ~ NA_real_
    ),
    Q_flags = case_when(
      qc_flag == "raw"                    ~ "AV",
      startsWith(qc_flag, "gf")           ~ paste0("EV: ", qc_flag),
      qc_flag == "unfilled"               ~ "MV",
      TRUE ~ qc_flag
    ),
    UNESCO_Q_level = case_when(
      qc_flag == "unfilled" ~ 9,
      TRUE                  ~ 1
    ),
    # if unfilled, there's no value to report
    stage_corrected     = if_else(qc_flag == "unfilled", NA_real_, stage_corrected),
    stat_avg_corrected  = if_else(qc_flag == "unfilled", NA_real_, stat_avg_corrected),
    stat_min_corrected  = if_else(qc_flag == "unfilled", NA_real_, stat_min_corrected),
    stat_max_corrected  = if_else(qc_flag == "unfilled", NA_real_, stat_max_corrected),
    stat_std_corrected  = if_else(qc_flag == "unfilled", NA_real_, stat_std_corrected)
  )

# ------------------------------------------------------------------------------
# 6. TIME COLUMNS -- convert to PST (Etc/GMT+8) and derive Year/Month/WaterYear
# ------------------------------------------------------------------------------

joined <- joined %>%
  mutate(
    Year  = year(measurement_time_pst),
    Month = month(measurement_time_pst),
    # Water year: Oct 1 - Sep 30, expressed to match existing water_year style.
    # Adjust format here if the network expects a single integer instead of
    # the "YYYY-YYYY" style already used upstream.
    WaterYear = if_else(
      Month >= 10,
      paste0(Year, "-", Year + 1),
      paste0(Year - 1, "-", Year)
    )
  )

# ------------------------------------------------------------------------------
# 7. BUILD OUTPUT COLUMNS with full sensor/site-prefixed names
# ------------------------------------------------------------------------------

prefix <- paste0("WtrLvl", sensor_gen, site_code)

out <- joined %>%
  transmute(
    `Measurement time` = format(measurement_time_pst, "%Y-%m-%d %H:%M:%S"),
    Year               = Year,
    Month              = Month,
    WaterYear          = WaterYear,
    !!paste0(prefix, "_Q_level")        := Q_level,
    !!paste0(prefix, "_Q_flags")        := Q_flags,
    !!paste0(prefix, "_UNESCO_Q_level") := UNESCO_Q_level,
    !!prefix                            := if_else(is.na(stage_corrected), NA_character_, sprintf("%.4f", stage_corrected)),
    !!paste0(prefix, "_Avg")            := if_else(is.na(stat_avg_corrected), NA_character_, sprintf("%.4f", stat_avg_corrected)),
    !!paste0(prefix, "_Min")            := if_else(is.na(stat_min_corrected), NA_character_, sprintf("%.4f", stat_min_corrected)),
    !!paste0(prefix, "_Max")            := if_else(is.na(stat_max_corrected), NA_character_, sprintf("%.4f", stat_max_corrected)),
    !!paste0(prefix, "_Std")            := if_else(is.na(stat_std_corrected), NA_character_, sprintf("%.4f", stat_std_corrected))
  )

# ------------------------------------------------------------------------------
# 8. WRITE OUTPUT with the 4-row network header block
# ------------------------------------------------------------------------------

col_names <- c("Measurement time", "Year", "Month", "WaterYear",
               paste0(prefix, "_Q_level"), paste0(prefix, "_Q_flags"),
               paste0(prefix, "_UNESCO_Q_level"), prefix,
               paste0(prefix, "_Avg"), paste0(prefix, "_Min"),
               paste0(prefix, "_Max"), paste0(prefix, "_Std"))

header_row1 <- col_names
header_row2 <- c("PST", "Year", "Month", "WaterYear",
                 "Quality level", "Quality flags", "UNESCO QC",
                 "m", "m", "m", "m", "m")
header_row3 <- c("measurementTime", "Year", "Month", "WaterYear",
                 rep(site_code, 8))
header_row4 <- c("measurementTime", "year", "month", "waterYear",
                 paste0(sensor_code, "_Lvl_QL"),
                 paste0(sensor_code, "_Lvl_QC"),
                 paste0(sensor_code, "_Lvl_UQL"),
                 paste0(sensor_code, "_Lvl"),
                 paste0(sensor_code, "_Lvl_Avg"),
                 paste0(sensor_code, "_Lvl_Min"),
                 paste0(sensor_code, "_Lvl_Max"),
                 paste0(sensor_code, "_Lvl_Std"))

con <- file(output_path, open = "wt")
writeLines(paste(header_row1, collapse = ","), con)
writeLines(paste(header_row2, collapse = ","), con)
writeLines(paste(header_row3, collapse = ","), con)
writeLines(paste(header_row4, collapse = ","), con)
write.table(out, con, sep = ",", row.names = FALSE, col.names = FALSE,
            quote = FALSE, na = "")
close(con)

message("Upload-ready file written to: ", output_path)