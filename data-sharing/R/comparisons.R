library(dplyr)
library(lubridate)
library(logger)


source("R/aquarius-utils.R")
source("R/utils.R")

Sys.setenv(RERDDAP_DEFAULT_URL = "https://catalogue.hakai.org/erddap/")

compare_stations <- function(stn_numbers, parameter = "Discharge") {
  # Initialize results data frame
  results <- data.frame(
    station_number = stn_numbers,
    status = character(length(stn_numbers)),
    stringsAsFactors = FALSE
  )

  test_columns <- c(
    "station_id",
    "time",
    "last_updated_stage_status",
    "stage",
    "discharge_rate"
  )

  # Process each station
  for (i in seq_along(stn_numbers)) {
    stn_number <- stn_numbers[i]

    tryCatch(
      {
        # Get time range for this station
        end_time <- read_last_measurements()$latest_time[
          read_last_measurements()$station_id == stn_number
        ] |>
          format("%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

        if (length(end_time) == 0 || is.na(end_time)) {
          end_time <- remember_min_last_passed_measurement()
        }

        start_time <- ymd_hms(end_time, tz = "UTC") - days(7)

        # Get data from both sources
        aquarius_df <- get_aquarius_data(
          stn_number,
          parameter,
          start_time,
          end_time
        )

        erddap_df <- get_station_measurements(
          start_time,
          stn_number,
          "HakaiWatershedsStreamStationsProvisional",
          test_columns
        )

        # Compare data
        comparison_df <- aquarius_df |>
          inner_join(erddap_df, by = c("timestamp" = "time")) |>
          filter(value != discharge_rate)

        # Station passes if there are no differences
        if (nrow(comparison_df) == 0) {
          results$status[i] <- "PASS"
        } else {
          results$status[i] <- "FAIL"
        }
      },
      error = function(e) {
        results$status[i] <<- "ERROR"
        log_warn("Error processing station {stn_number}: {e$message}")
      }
    )
  }

  # Print results table
  cat("\nStation Comparison Results:\n")
  cat("=========================\n")
  for (i in 1:nrow(results)) {
    cat(sprintf("%-12s: %s\n", results$station_number[i], results$status[i]))
  }
  cat("\n")

  # Check if any stations failed
  failed_stations <- results$station_number[
    results$status %in% c("FAIL", "ERROR")
  ]

  if (length(failed_stations) > 0) {
    stop(sprintf(
      "The following stations failed the comparison: %s",
      paste(failed_stations, collapse = ", ")
    ))
  }

  return(results)
}

result <- compare_stations(c("H08KC0626", "H08KC0844"))
