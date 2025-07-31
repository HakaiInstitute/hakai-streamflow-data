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
    store = disk(glue::glue("historical-data/")),
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
  group_by(station_id) |>
  summarise(
    n = n(),
    first_time = min(time),
    last_time = max(time)
  ) |>
  collect()
