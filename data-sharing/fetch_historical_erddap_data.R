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

# Configuration
dataset_id <- "HakaiWatershedsStreamStationsProvisional"

columns <- c(
  "station_id",
  "station_description",
  "longitude",
  "latitude",
  "time",
  "last_updated_stage_status",
  "stage",
  "stage_ql",
  "stage_qc",
  "discharge_rate",
  "discharge_volume_ql",
  "discharge_volume_qc"
)


get_historical_erddap_data <- function(station_id, date) {
  time_param <- glue::glue("time<{date}")
  station_id_param <- glue::glue('station_id="{station_id}"')

  file <- rerddap::tabledap(
    "HakaiWatershedsStreamStationsProvisional",
    fields = columns,
    store = disk(glue::glue("data-sharing/historical-data/")),
    fmt = 'parquet',
    time_param,
    station_id_param
  )

  fs::file_move(
    attr(file, "path"),
    glue("historical-data/historical-{station_id}.parquet")
  )
}


get_historical_erddap_data("H08KC0626", "2025-07-06 19:05:00") # active
get_historical_erddap_data("H08KC0693", "2025-07-06 19:05:00")
get_historical_erddap_data("H08KC0703", "2025-07-06 19:05:00") # active
get_historical_erddap_data("H08KC0708", "2025-07-06 19:05:00")
get_historical_erddap_data("H08KC0819", "2025-07-06 19:05:00")
get_historical_erddap_data("H08KC0844", "2025-07-06 19:05:00") # active
get_historical_erddap_data("H08KC1015", "2025-07-06 19:05:00") # active

## check stations
library(dplyr)
library(arrow)

open_dataset("historical-data/") |>
  mutate(file = add_filename()) |>
  group_by(station_id, file, latitude, longitude) |>
  summarise(
    n = n(),
    first_time = min(time),
    last_time = max(time),
  ) |>
  collect() |>
  ungroup() |>
  mutate(file = basename(file)) |>
  arrange(station_id) |>
  gt::gt()

## send along to FTP server
ftp_upload <- function(file_name) {
  ftp_url <- Sys.getenv("FTP_URL")
  ftp_url_plus_name <- paste0(ftp_url, basename(file_name))
  resp <- curl::curl_upload(
    file = file_name,
    url = ftp_url_plus_name,
    verbose = TRUE,
    reuse = FALSE
  )
  resp
}

ftp_upload("historical-data/historical-H08KC0626.parquet")
ftp_upload("historical-data/historical-H08KC0693.parquet")
ftp_upload("historical-data/historical-H08KC0703.parquet")
ftp_upload("historical-data/historical-H08KC0708.parquet")
ftp_upload("historical-data/historical-H08KC0819.parquet")
ftp_upload("historical-data/historical-H08KC0844.parquet")
ftp_upload("historical-data/historical-H08KC1015.parquet")
