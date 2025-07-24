library(dplyr)
library(lubridate)

source("R/aquarius-utils.R")
source("R/utils.R")

Sys.setenv(RERDDAP_DEFAULT_URL = "https://catalogue.hakai.org/erddap/")

stn_number <- "H08KC0626"
parameter <- "Discharge"
end_time <- remember_min_last_passed_measurement()
start_time <- ymd_hms(end_time, tz = "UTC") - days(7)


test_columns <- c(
  "station_id",
  "time",
  # "last_updated_stage_status",
  # "stage",
  "discharge_rate"
)

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

aquarius_df |>
  inner_join(erddap_df, by = c("timestamp" = "time")) |>
  filter(value != discharge_rate)
