library(rerddap)
library(logger)
library(glue)
library(nanoparquet)
library(here)
library(sentryR)

Sys.setenv(RERDDAP_DEFAULT_URL = "https://catalogue.hakai.org/erddap/")
log_info(
  "Using the '{Sys.getenv('RERDDAP_DEFAULT_URL')}' ERDDAP instance"
)

source(here("data-sharing/R/utils.R"))

if (is_gha()) {
  logger::log_info("Configuring sentry")
  configure_sentry(
    dsn = Sys.getenv("SENTRY_DSN"),
    app_name = 'data-sharing',
    app_version = Sys.getenv("GITHUB_SHA"),
    environment = 'ci',
    tags = list(
      repository = Sys.getenv("GITHUB_REPOSITORY"),
      branch = Sys.getenv("GITHUB_REF_NAME"),
      workflow = Sys.getenv("GITHUB_WORKFLOW")
    )
  )
}

# Configuration
dataset_id <- "HakaiWatershedsStreamStationsProvisional"

columns <- c(
  "station_id",
  "station_description",
  "longitude",
  "latitude",
  "time",
  "last_updated_lvl_status",
  "pls_lvl",
  "pls_lvl_ql",
  "pls_lvl_qc",
  "discharge_rate",
  "discharge_volume_ql",
  "discharge_volume_qc"
)

make_ftp_safe_filename <- function(df, dataset_id) {
  time_last_pass <- format(min(df$time), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  file_name <- glue("{dataset_id}_since_{time_last_pass}.parquet")
  ## make ftp safe
  gsub(":", "-", file_name)
}

# Fetch data
tryCatch(
  {
    last_measurements <- read_last_measurements()
    station_data <- fetch_station_data(last_measurements)
    file_name <- make_ftp_safe_filename(station_data, dataset_id)
    write_parquet(station_data, file_name)

    log_info("Retrieved {nrow(station_data)} rows in {file_name}")
  },
  error = function(e) {
    capture_sentry_exception(e)
    log_info("Error:", as.character(e), "\n")
  }
)


tryCatch(
  {
    # if (resp$status_code == 226) {
    if (TRUE) {
      log_info(file_name, " successfully transferred")
      record_last_passed_measurements(file_name)
    }
  },
  error = function(e) {
    capture_sentry_exception(e)
    log_info("Error:", as.character(e), "\n")
  }
)
