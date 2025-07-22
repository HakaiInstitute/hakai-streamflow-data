build_aquarius_url <- function(stn_number, parameter, start_time, end_time) {
  parameter <- tools::toTitleCase(parameter)
  data_set <- glue::glue("{parameter}.Working@{stn_number}")

  httr2::url_modify(
    "https://bcmoe-prod.aquaticinformatics.net/Export/DataSet",
    query = list(
      DataSet = data_set,
      Calendar = "CALENDARYEAR",
      StartTime = start_time,
      EndTime = end_time,
      DateRange = "Custom",
      UnitID = 350,
      Conversion = "Instantaneous",
      IntervalPoints = "PointsAsRecorded",
      ApprovalLevels = "False",
      Qualifiers = "False",
      Step = 1,
      ExportFormat = "csv",
      Compressed = "false",
      RoundData = "False",
      GradeCodes = "False",
      InterpolationTypes = "False",
      Timezone = 0
    )
  )
}

get_aquarius_data <- function(...) {
  url <- build_aquarius_url(...)
  tfile <- tempfile(fileext = ".csv")
  resp <- httr2::request(url) |>
    httr2::req_user_agent(
      "https://github.com/HakaiInstitute/hakai-streamflow-data"
    ) |>
    httr2::req_perform(path = tfile)

  httr2::resp_check_status(resp)

  readr::read_csv(
    tfile,
    skip = 6,
    col_types = "Td",
    col_names = c("timestamp", "value")
  )
}
