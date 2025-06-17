library(rerddap)
library(logger)
library(glue)
library(nanoparquet)
library(here)

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
  "last_updated_lvl_status",
  "pls_lvl",
  "pls_lvl_ql",
  "pls_lvl_qc",
  "discharge_rate",
  "discharge_volume_ql",
  "discharge_volume_qc"

)


# Format time constraint for ERDDAP
time_constraint <- remember_min_last_passed_measurement()
time_param <- glue("last_updated_lvl_time>={time_constraint}")
log_info("Querying '{dataset_id}' dataset since {time_constraint} (UTC)}")

# Fetch data
tryCatch(
  {
    discharge_output <- tabledap(
      dataset_id,
      fields = columns,
      store = disk("."),
      fmt = 'parquet',
      time_param
    )

    discharge_output <- remember_last_passed_measurements(discharge_output)
    
    file_name <- glue("{dataset_id}_since_{time_constraint}.parquet")
    ## make ftp safe
    file_name <- gsub(":", "-", file_name)
    write_parquet(discharge_output, file_name)

    log_info("Retrieved {nrow(discharge_output)} rows in {file_name}")
  },
  error = function(e) {
    log_info("Error:", as.character(e), "\n")
  }
)


tryCatch(
  {
    
    # if (resp$status_code == 226) {
    if (TRUE) {
      log_info(filename, " successfully transferred")
      record_last_passed_measurements(file_name)
    }
    
  },
  error = function(e) {
    log_info("Error:", as.character(e), "\n")
  }
)
