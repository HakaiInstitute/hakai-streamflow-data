#' Read last measurement times from CSV file
#'
#' Reads a CSV containing the latest measurement times for each station
#' and converts time columns to UTC POSIXct format.
#'
#' @param ... Arguments passed to read.csv()
#' @return Data frame with latest_time and record_time as UTC POSIXct
read_last_measurements <- function(...) {
  latest_times <- read.csv(...)
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
  latest_times <- aggregate(time ~ station_id, data = df, FUN = max)
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

#' Filter measurements to exclude already processed data
#'
#' Filters a data frame to only include measurements newer than the last
#' successfully processed measurements for each station. If no tracking
#' file exists, returns all measurements.
#'
#' @param df Data frame with columns station_id and time
#' @return Filtered data frame excluding previously processed measurements
remember_last_passed_measurements <- function(df) {
  last_passed_measurements <- here::here("data-sharing/last_passed_measurements.csv")
  is_present <- file.exists(last_passed_measurements)

  if (is_present) {
    latest_times <- read_last_measurements(last_passed_measurements)

    merged_data <- merge(
      df,
      latest_times,
      by = "station_id",
      all.x = TRUE
    )
    filtered_measurements <- merged_data[
      is.na(merged_data$latest_time) |
        merged_data$time > merged_data$latest_time,
    ]
    # Remove the latest_time column
    filtered_measurements$latest_time <- NULL
  } else {
    filtered_measurements <- df
  }
  return(filtered_measurements)
}

#' Get the earliest time to query for new measurements
#'
#' Finds the maximum (most recent) time from all stations' last processed
#' measurements and returns it as an ISO 8601 formatted string in UTC.
#'
#' @return Character string of the latest measurement time in ISO 8601 format
remember_min_last_passed_measurement <- function() {
  last_passed_measurements <- here::here("data-sharing/last_passed_measurements.csv")
  latest_times <- read_last_measurements(last_passed_measurements)
  max_time <- max(latest_times$latest_time)
  format(max_time, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}