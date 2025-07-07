#' Read last measurement times from CSV file
#'
#' Reads a CSV file containing the latest measurement times for each station
#' and converts time columns to UTC POSIXct format. This function is used to
#' track which measurements have already been successfully processed.
#'
#' @return A data frame with columns:
#'   \item{station_id}{Station identifier}
#'   \item{latest_time}{Most recent measurement time as UTC POSIXct}
#'   \item{record_time}{When this record was created as UTC POSIXct}
read_last_measurements <- function() {
  last_passed_measurements <- here::here(
    "data-sharing/last_passed_measurements.csv"
  )
  is_present <- file.exists(last_passed_measurements)

  if (!is_present) {
    latest_times <- create_first_measurement()
  } else {
    latest_times <- read.csv(last_passed_measurements)
    latest_times$latest_time <- as.POSIXct(latest_times$latest_time, tz = "UTC")
    latest_times$record_time <- as.POSIXct(latest_times$record_time, tz = "UTC")
  }

  latest_times
}

create_first_measurement <- function() {
  record_time <- as.POSIXct(
    format(Sys.time(), tz = "UTC"),
    tz = "UTC"
  )
  latest_time <- record_time - 86400

  data.frame(
    station_id = c("H08KC0626", "H08KC0703", "H08KC0844", "H08KC1015"),
    record_time = record_time,
    latest_time = latest_time
  )
}

#' Record the latest measurement times for each station
#'
#' Reads a parquet file containing measurement data, finds the maximum time
#' for each station, and saves this information to a CSV file. This creates
#' a checkpoint for tracking which measurements have been successfully processed.
#'
#' @param file_name Character. Path to parquet file containing measurement data
#'   with columns 'time' and 'station_id'
#' @return Invisibly returns a data frame with latest times per station
record_last_passed_measurements <- function(file_name) {
  if (!file.exists(file_name)) {
    stop("File not found: ", file_name, call. = FALSE)
  }

  df <- nanoparquet::read_parquet(file_name)

  # Find the latest time per station
  latest_times <- aggregate(time ~ station_id, data = df, FUN = max)
  names(latest_times)[2] <- "latest_time"

  # Add timestamp of when this record was created
  latest_times$record_time <- as.POSIXct(
    format(Sys.time(), tz = "UTC"),
    tz = "UTC"
  )

  # Save to CSV
  write.csv(
    latest_times,
    here::here("data-sharing/last_passed_measurements.csv"),
    row.names = FALSE
  )

  logger::log_info(
    "Recorded last successful export for {nrow(latest_times)} stations"
  )

  return(invisible(latest_times))
}

#' Get the earliest time to query for new measurements
#'
#' Finds the minimum (earliest) time from all stations' last processed
#' measurements. This ensures no data is missed when querying for new
#' measurements across multiple stations.
#'
#' @return Character string of the earliest measurement time in ISO 8601 format (UTC)
remember_min_last_passed_measurement <- function() {
  latest_times <- read_last_measurements()
  min_time <- min(latest_times$latest_time)
  format(min_time, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

#' Fetch measurement data for multiple stations
#'
#' Retrieves measurement data for all stations using their respective
#' last measurement times as starting points. Combines results into
#' a single data frame.
#'
#' @param last_measurements Data frame with columns 'station_id' and 'latest_time'
#'   as returned by \code{read_last_measurements()}
#' @return Data frame containing combined measurement data from all stations
fetch_station_data <- function(last_measurements) {
  station_data_list <- mapply(
    get_station_measurements,
    last_measurements$latest_time,
    last_measurements$station_id,
    SIMPLIFY = FALSE
  )

  do.call(rbind, station_data_list)
}

#' Get measurement data for a single station
#'
#' Queries the ERDDAP dataset for measurements from a specific station
#' starting from a given date/time. Uses the rerddap package to fetch
#' data in parquet format.
#'
#' @param date POSIXct or character. Start date/time for data retrieval
#' @param station_id Character. Station identifier to query
#' @return Data frame containing measurement data for the specified station,
#'   or NULL if query fails
get_station_measurements <- function(date, station_id) {
  time_param <- glue::glue("last_updated_lvl_time>={date}")
  station_id_param <- glue::glue('station_id="{station_id}"')

  logger::log_info(
    "Querying '{dataset_id}' dataset for {station_id} since {date}"
  )

  tryCatch(
    {
      rerddap::tabledap(
        dataset_id,
        fields = columns,
        fmt = 'parquet',
        time_param,
        station_id_param
      )
    },
    error = function(e) {
      logger::log_error(
        "Failed to fetch data for station {station_id}: {e$message}"
      )
      return(NULL)
    }
  )
}


capture_sentry_exception <- function(e) {
  logger::log_info("Creating sentry alert.")
  if (is_main() && is_gha()) {
    sentryR::capture_exception(
      error = e,
      extra = list(
        run_url = Sys.getenv("GITHUB_RUN_URL"),
        commit = Sys.getenv("GITHUB_SHA")
      )
    )
  }
}


make_ftp_safe_filename <- function(df, dataset_id) {
  time_last_pass <- format(min(df$time), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  file_name <- glue::glue("{dataset_id}_since_{time_last_pass}.parquet")
  ## make ftp safe
  gsub(":", "-", file_name)
}


is_gha <- function() {
  Sys.getenv("GITHUB_ACTIONS") == "true"
}

is_main <- function() {
  Sys.getenv("GITHUB_REF_NAME") == "main"
}
