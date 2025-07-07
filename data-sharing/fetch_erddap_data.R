library(rerddap)
library(logger)
library(glue)
library(nanoparquet)
library(here)
library(sentryR)
library(curl)

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
    log_error(as.character(e))
  }
)

if (is_gha() && is_main()) {
  tryCatch(
    {
      log_info("Uploading ", file_name)
      ftp_url <- Sys.getenv("FTP_URL")
      resp <- curl_upload(
        url = paste0(ftp_url, file_name),
        file = file_name
      )

      if (resp$status_code >= 200 && resp$status_code < 300) {
        log_info(file_name, " successfully transferred")
        record_last_passed_measurements(file_name)
      }
    },
    error = function(e) {
      capture_sentry_exception(e)
      log_error(as.character(e))
    }
  )
}

