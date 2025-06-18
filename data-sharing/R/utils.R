#' Read last measurement times from CSV file
#'
#' Reads a CSV containing the latest measurement times for each station
#' and converts time columns to UTC POSIXct format.
#'
#' @param ... Arguments passed to read.csv()
#' @return Data frame with latest_time and record_time as UTC POSIXct
read_last_measurements <- function() {
  last_passed_measurements <- here::here("data-sharing/last_passed_measurements.csv")
  is_present <- file.exists(last_passed_measurements)

  if (!is_present) {
    stop("No previously measurements", call. = FALSE)
  }
  latest_times <- read.csv(last_passed_measurements)
  latest_times$latest_time <- as.POSIXct(latest_times$latest_time, tz = "UTC")
  latest_times$record_time <- as.POSIXct(latest_times$record_time, tz = "UTC")
  latest_times
}

#' Record the latest measurement times for each station
#'
#' Reads a parquet file, finds the maximum time for each station,
#' and saves this information to a CSV file for tracking successful exports.
#'
#' @param file_name Path to parquet file containing measurement data
#' @return Invisibly returns the latest times data frame
record_last_passed_measurements <- function(file_name) {
  df <- nanoparquet::read_parquet(file_name)
  # find the latest time per station
  latest_times <- aggregate(time ~ station_id, data = df, FUN = max)
  # then find the earliest of those
  names(latest_times)[2] <- "latest_time"
  latest_times$record_time <- as.POSIXct(
    format(Sys.time(), tz = "UTC"),
    tz = "UTC"
  )
  write.csv(
    latest_times,
    here("data-sharing/last_passed_measurements.csv"),
    row.names = FALSE
  )
  logger::log_info("Recorded last successful export")

  return(invisible(latest_times))
}

#' Get the earliest time to query for new measurements
#'
#' Finds the maximum (most recent) time from all stations' last processed
#' measurements and returns it as an ISO 8601 formatted string in UTC.
#'
#' @return Character string of the latest measurement time in ISO 8601 format
remember_min_last_passed_measurement <- function() {
  latest_times <- read_last_measurements()
  max_time <- min(latest_times$latest_time)
  format(max_time, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}


fetch_station_data <- function(last_measurements) {
  station_data_list <- mapply(
    get_station_measurements, 
    last_measurements$latest_time, 
    last_measurements$station_id, 
    SIMPLIFY = FALSE
  )
  do.call(rbind, station_data_list)
}

get_station_measurements <- function(date, station_id) {
  time_param <- glue::glue("last_updated_lvl_time>={date}")
  station_id_param <- glue::glue('station_id=\"{station_id}\"')
  logger::log_info("Querying '{dataset_id}' dataset for {station_id} since {date} (UTC)}")

  rerddap::tabledap(
      dataset_id,
      fields = columns,
      fmt = 'parquet',
      time_param,
      station_id_param
    )
}