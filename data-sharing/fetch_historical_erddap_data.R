library(rerddap)
library(logger)
library(glue)
library(nanoparquet)
library(here)
library(sentryR)
library(curl)

Sys.setenv(RERDDAP_DEFAULT_URL = "https://catalogue.hakai.org/erddap/")
options(scipen = 999) # Disable scientific notation
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
  dest_file <- glue("historical-data/Hakai-historical-{station_id}.parquet")

  fs::file_move(
    attr(file, "path"),
    dest_file
  )
  dest_dir <- fs::path_ext_remove(dest_file)
  fs::dir_create(dest_dir)

  df <- nanoparquet::read_parquet(dest_file)
  chunk_size <- 10000
  total_rows <- nrow(df)
  n_chunks <- ceiling(total_rows / chunk_size)

  for (i in 1:n_chunks) {
    print(glue("Processing chunk {i} of {n_chunks}"))
    start_row <- (i - 1) * chunk_size + 1
    end_row <- min(i * chunk_size, total_rows)

    chunk <- df[start_row:end_row, ]

    filename <- glue(
      "{dest_dir}/{dest_dir}_chunk{i}_{start_row}_to_{end_row}.parquet"
    )
    nanoparquet::write_parquet(chunk, filename)
  }
  unlink(dest_file) # Remove the original file after processing
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
