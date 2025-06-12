library(rerddap)
library(logger)
library(glue)

Sys.setenv(RERDDAP_DEFAULT_URL = "https://catalogue.hakai.org/erddap/")
log_info(glue(
  "Using the '{Sys.getenv('RERDDAP_DEFAULT_URL')}' ERDDAP instance"
))

# Configuration
dataset_id <- "HakaiWatershedsStreamStationsProvisional"

columns <- c(
  "station",
  "longitude",
  "latitude",
  "time",
  "pls_lvl",
  "pls_lvl_ql",
  "pls_lvl_qc"
)


yesterday <- Sys.time() - as.difftime(1, units = "days")
time_constraint <- format(yesterday, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
# Format time constraint for ERDDAP
time_param <- paste0("time>=", time_constraint)

log_info(glue("Querying '{dataset_id}' dataset since {time_constraint} (UTC)}"))

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

    temp_file_name <- attr(discharge_output, "path")
    file_name <- glue("{dataset_id}_since_{time_constraint}.parquet")
    ## make ftp safe
    file_name <- gsub(":", "-", file_name)
    file.rename(temp_file_name, file_name)


    log_info(glue("Retrieved {nrow(discharge_output)} rows in {file_name}"))
  },
  error = function(e) {
    log_info("Error:", as.character(e), "\n")
  }
)
