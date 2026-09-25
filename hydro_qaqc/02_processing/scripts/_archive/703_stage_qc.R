library(readr)
library(dplyr)

# ---- Load raw PT4 (ssn703_d / PLS4) stage file ----
# 4 header rows: variable name, units, siteID, internal variable code
raw_path <- "C:/Users/Emily/Documents/git-repos/hydro_qaqc/01_raw/SSN703/ssn703_d.csv"

ssn703_d <- read_csv(
  raw_path,
  skip = 4,
  col_names = c(
    "measurement_time",
    "year",
    "month",
    "water_year",
    "stage_raw",
    "stage_avg",
    "stage_min",
    "stage_max",
    "stage_std"
  ),
  col_types = cols(
    measurement_time = col_datetime(format = "%Y-%m-%d %H:%M:%S"),
    year = col_integer(),
    month = col_character(),
    water_year = col_character(),
    stage_raw = col_double(),
    stage_avg = col_double(),
    stage_min = col_double(),
    stage_max = col_double(),
    stage_std = col_double()
  ),
  locale = locale(tz = "Etc/GMT+8")
)

# ---- Trim to RC5/PT4 era: 2021-09-15 00:00 onwards ----
rc5_start <- as.POSIXct("2021-09-15 00:00:00", tz = "Etc/GMT+8")

ssn703_d_rc5 <- ssn703_d %>%
  filter(measurement_time >= rc5_start) %>%
  arrange(measurement_time)

# ---- Quick sanity checks ----
cat("Rows:", nrow(ssn703_d_rc5), "\n")
cat("Date range:", format(min(ssn703_d_rc5$measurement_time)), "to",
    format(max(ssn703_d_rc5$measurement_time)), "\n")
cat("NAs in stage_avg:", sum(is.na(ssn703_d_rc5$stage_avg)), "\n")

# Check timestep is consistently 300s (5 min) - flag any irregular gaps
gaps <- diff(as.numeric(ssn703_d_rc5$measurement_time))
cat("Timestep table (seconds):\n")
print(table(gaps))

# ---- Save trimmed file for next step ----
write_csv(ssn703_d_rc5, "C:/Users/Emily/Documents/git-repos/hydro_qaqc/01_raw/SSN703/ssn703_d_rc5_trimmed.csv")