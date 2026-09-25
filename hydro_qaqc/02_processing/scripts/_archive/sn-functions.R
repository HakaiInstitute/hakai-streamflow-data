# ============================================================
# sn_* -- Hakai Sensor Network read functions
# ============================================================
# Function naming and style modeled on hydrocan/tidyhydat:
#   <pkg-prefix>_<verb>_<noun>(), snake_case throughout, tibble
#   in -> tibble out, station argument accepts a vector, glue
#   for query building, dplyr/tidyr for reshaping.
#
# Column naming: everything uses `measurement_time`, end to end.
# The raw sn/views endpoint calls it `measurementTime` (camelCase)
# -- that's just this one endpoint's JSON field naming -- but the
# sn_qc schema (the actual destination for QC'd data) consistently
# uses `measurement_time` (snake_case), matching every other column
# there (measurement_name, quality_level, qc_flag, qc_by,
# recorded_time). So `measurement_time` is used everywhere in this
# pipeline; the rename from the raw API's `measurementTime` happens
# once, at the very first step, and nowhere else.
#
# Source this file, then call sn_read_station() -- see
# examples at the bottom.

library(tidyverse)
library(glue)
library(hakaiApi)


#' Connect to the Hakai Sensor Network API
#'
#' Thin wrapper around [hakaiApi::Client] so callers don't need to
#' know the API root. Triggers a browser login on first use;
#' credentials are cached after that.
#'
#' @param api_root Base URL of the Hakai API.
#' @return A `hakaiApi::Client` object, to be passed as `client` to
#'   the other `sn_*` functions.
#' @export
sn_connect <- function(api_root = "https://portal.hakai.org") {
  hakaiApi::Client$new(api_root)
}


#' Read raw sensor values for one station
#'
#' Pulls value columns from the `sn/views` endpoint for a single
#' site/view and reshapes them to long format.
#'
#' @param client A client from [sn_connect()].
#' @param site Sensor network site code, e.g. `"SSN703US"`.
#' @param view Sample view name, e.g. `"5minuteSamples"`.
#' @param components Character vector of component/variable names
#'   to pull, e.g. `c("PLS_Lvl", "PLS_Temp")`.
#' @param start_date,end_date Date range as `"YYYY-MM-DD"` strings.
#' @return A long tibble with columns `measurement_time`, `site`,
#'   `variable`, `value`. If the date range has no matching rows at
#'   all (a real, non-error outcome -- e.g. a sensor that isn't
#'   active yet/anymore for that window), returns a 0-row tibble
#'   with those same columns rather than erroring.
#' @export
sn_read_values <- function(client, site, view, components, start_date, end_date) {
  field_names <- paste(site, components, sep = ":")
  fields <- paste(c("measurementTime", field_names), collapse = ",")

  query <- glue(
    "api/sn/views/{site}:{view}?fields={fields}",
    "&measurementTime>{start_date}",
    "&measurementTime<{end_date}",
    "&limit=-1"
  )

  result <- client$get(query)

  if (!"measurementTime" %in% names(result)) {
    message(glue(
      "No rows returned for {site}:{view} between {start_date} and {end_date} -- ",
      "returning an empty tibble rather than erroring."
    ))
    return(tibble(measurement_time = as.POSIXct(character()), site = character(),
                   variable = character(), value = numeric()))
  }

  result %>%
    rename(measurement_time = measurementTime) %>%
    pivot_longer(
      cols = -measurement_time,
      names_to = c("site", "variable"),
      names_sep = ":",
      values_to = "value"
    ) %>%
    arrange(variable, measurement_time)
}


#' Derive the underlying QC table name for a site/view
#'
#' The QC endpoint (`sn/qc/:tableName`) does NOT use the
#' `Site:Component` naming from `sn/views` -- it wants a snake_case
#' name, e.g. `"ssn703us_5minute"`. Confirmed by testing against the
#' live API: lowercase, strip anything before the last `/`, drop
#' `...samples` onward, replace `:` with `_`.
#'
#' @param site,view As in [sn_read_values()].
#' @return A single string: the QC table name.
#' @export
sn_qc_table_name <- function(site, view) {
  glue("{site}:{view}") %>%
    tolower() %>%
    str_remove(".*/") %>%
    str_remove("samples.*$") %>%
    str_replace_all(":", "_")
}


#' Read manual QC flags for a station's underlying table
#'
#' Pulls from `sn/qc/:tableName` (documented at
#' <https://hakaiinstitute.github.io/hakai-api/endpoints/>). One
#' table can hold QC history for several `measurement_name` values
#' at once -- filter with [sn_join_qc()], not here.
#'
#' @param client A client from [sn_connect()].
#' @param table_name From [sn_qc_table_name()].
#' @param start_date,end_date Date range as `"YYYY-MM-DD"` strings.
#' @return A tibble with (at least) `measurement_name`,
#'   `measurement_time`, `quality_level`, `qc_flag`. Empty tibble
#'   with those columns if the table can't be reached.
#' @export
sn_read_qc <- function(client, table_name, start_date, end_date) {
  query <- glue(
    "api/sn/qc/{table_name}?measurement_time>{start_date}",
    "&measurement_time<{end_date}",
    "&limit=-1"
  )
  tryCatch(
    client$get(query),
    error = function(e) {
      warning(glue("Couldn't reach QC table '{table_name}': {conditionMessage(e)}"))
      tibble(measurement_name = character(), measurement_time = as.POSIXct(character()),
             quality_level = numeric(), qc_flag = character())
    }
  )
}


#' Join QC flags onto a values tibble
#'
#' Merges only the `measurement_name`s explicitly listed in
#' `qc_map` -- a component with no entry in `qc_map` is left
#' unmerged on purpose (e.g. `PLS_Lvl`, which has no QC history
#' under that name; only its calculated derivative `Stage` does,
#' and that's a different variable, not a stand-in for it).
#'
#' @param values A tibble from [sn_read_values()].
#' @param qc_flags A tibble from [sn_read_qc()].
#' @param qc_map Named character vector: `component = measurement_name`,
#'   e.g. `c(PLS_Temp = "PLS_Temp")`. Only components named here get
#'   QC columns merged in.
#' @return `values` with `quality_level`/`qc_flag` columns added.
#'   Rows for unmapped components (or with no matching QC row) get
#'   `NA` in both -- that's expected, not a merge failure.
#' @export
sn_join_qc <- function(values, qc_flags, qc_map) {
  unmapped <- setdiff(unique(values$variable), names(qc_map))
  if (length(unmapped) > 0) {
    available <- if (nrow(qc_flags) > 0) {
      paste(unique(qc_flags$measurement_name), collapse = ", ")
    } else {
      "(no QC data found for this table/date range)"
    }
    message(glue(
      "No qc_map entry for: {paste(unmapped, collapse = ', ')} -- ",
      "these stay NA. measurement_name values available in this table: {available}"
    ))
  }

  if (nrow(qc_flags) == 0 || length(qc_map) == 0) {
    return(values %>% mutate(quality_level = NA_real_, qc_flag = NA_character_))
  }

  qc_clean <- qc_flags %>%
    filter(measurement_name %in% qc_map) %>%
    mutate(variable = names(qc_map)[match(measurement_name, qc_map)]) %>%
    select(measurement_time, variable, quality_level, qc_flag)

  values %>% left_join(qc_clean, by = c("measurement_time", "variable"))
}


#' Read values + QC flags for one or more stations
#'
#' The main entry point -- like `hc_read_daily_flows()` in
#' hydrocan or `hy_daily_flows()` in tidyhydat, `site` accepts a
#' vector so one call can cover several stations at once.
#'
#' @param client A client from [sn_connect()].
#' @param site Character vector of site codes.
#' @param view Sample view name (recycled across all sites).
#' @param components Character vector of component names (recycled
#'   across all sites).
#' @param qc_map Named character vector, `component = measurement_name`
#'   (recycled across all sites) -- see [sn_join_qc()].
#' @param start_date,end_date Date range as `"YYYY-MM-DD"` strings
#'   (recycled across all sites).
#' @return A long tibble across all requested sites, with columns
#'   `measurement_time`, `site`, `variable`, `value`, `quality_level`,
#'   `qc_flag`.
#' @export
sn_read_station <- function(client, site, view = "5minuteSamples",
                               components, qc_map = character(),
                               start_date, end_date) {
  map_dfr(site, function(s) {
    message(glue("--- {s}:{view} ---"))

    values <- sn_read_values(client, s, view, components, start_date, end_date)

    table_name <- sn_qc_table_name(s, view)
    qc_flags <- sn_read_qc(client, table_name, start_date, end_date)

    sn_join_qc(values, qc_flags, qc_map)
  })
}


#' Plot a station's values with QC flags highlighted
#'
#' @param data A tibble from [sn_read_station()].
#' @return A ggplot object.
#' @export
sn_plot_station <- function(data) {
  data %>%
    ggplot(aes(x = measurement_time, y = value, color = !is.na(qc_flag))) +
    geom_line(aes(group = interaction(site, variable)), color = "grey60") +
    geom_point(data = ~ filter(.x, !is.na(qc_flag)), size = 1) +
    facet_grid(variable ~ site, scales = "free_y") +
    labs(x = NULL, y = NULL, color = "Flagged") +
    theme(legend.position = "bottom")
}


# ================================================================
# Examples
# ================================================================
# client <- sn_connect()
#
# # Single station:
# ssn703 <- sn_read_station(
#   client,
#   site       = "SSN703US",
#   components = c("PLS_Lvl", "PLS_Temp"),
#   qc_map     = c(PLS_Temp = "PLS_Temp"),  # PLS_Lvl has no QC match -- omitted on purpose
#   start_date = "2026-07-31",
#   end_date   = "2026-08-02"
# )
# sn_plot_station(ssn703)
#
# # Several stations in one call, like hy_daily_flows(station_number = c(...)):
# multi <- sn_read_station(
#   client,
#   site       = c("SSN703US", "SSN626US"),
#   components = c("PLS_Lvl", "PLS_Temp"),
#   qc_map     = c(PLS_Temp = "PLS_Temp"),
#   start_date = "2026-07-31",
#   end_date   = "2026-08-02"
# )
