# =============================================================================
# stage_qc_functions.R -- the reusable function library for stage QC
# =============================================================================
# Source this ONE file, then run run_stage_qc_ssn703.R (which sources
# stage_qc_pipeline.R internally).
#
# Replaces, with no loss of functionality:
#   qc-functions.R, sn-functions.R, qc_diagnostics.R, qc_plot_diagnostic.R,
#   qc_review.R, plot_sensor_qc_flags.R, and the loader half of
#   01_load_stage_data.R / the overlap plots of 02_inspect_stage.R.
#
# Naming follows hydrocan / tidyhydat: <prefix>_<verb>_<noun>(), snake_case,
# tibble in -> tibble out. `sn_` = Hakai Sensor Network API access;
# `stage_` = local raw-file loading; `qc_` = quality-control diagnostics,
# flagging support, and review. `measurement_time` is the one time-column
# name used end to end (the raw sn/views endpoint's camelCase
# `measurementTime` is renamed once, at read).
#
# Sections:
#   A. sn_*    -- Hakai API download (default input path)
#   B. stage_* -- local raw CSV loader (offline / reprocessing fallback)
#   C. qc_*    -- pre-threshold diagnostics (gap lengths, rate of change)
#   D. qc_*    -- pre-flagging visual triage (qc_plot_diagnostic, qc_plot_overlap)
#   E. qc_*    -- post-flagging review & verification
# =============================================================================

library(tidyverse)
library(glue)
library(hakaiApi)
library(roll)
library(patchwork)
library(plotly)


# #############################################################################
# A. sn_* -- Hakai Sensor Network API download
# #############################################################################

#' Connect to the Hakai Sensor Network API
#'
#' Thin wrapper around [hakaiApi::Client] so callers don't need to know the
#' API root. Triggers a browser login on first use; credentials are cached
#' after that.
#'
#' @param api_root Base URL of the Hakai API.
#' @return A `hakaiApi::Client` object, to be passed as `client` to the
#'   other `sn_*` functions.
#' @export
sn_connect <- function(api_root = "https://portal.hakai.org") {
  hakaiApi::Client$new(api_root)
}


#' Read raw sensor values for one station
#'
#' Pulls value columns from the `sn/views` endpoint for a single site/view
#' and reshapes them to long format.
#'
#' @param client A client from [sn_connect()].
#' @param site Sensor network site code, e.g. `"SSN703US"`.
#' @param view Sample view name, e.g. `"5minuteSamples"`.
#' @param components Character vector of component/variable names to pull,
#'   e.g. `c("PLS_Lvl", "PLS_Temp")`.
#' @param start_date,end_date Date range as `"YYYY-MM-DD"` strings.
#' @return A long tibble with columns `measurement_time`, `site`,
#'   `variable`, `value`. If the date range has no matching rows at all (a
#'   real, non-error outcome), returns a 0-row tibble with those same
#'   columns rather than erroring.
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
#' The QC endpoint wants a snake_case name, e.g. `"ssn703us_5minute"`:
#' lowercase, strip anything before the last `/`, drop `...samples` onward,
#' replace `:` with `_`.
#'
#' @param site,view As in [sn_read_values()].
#' @return A single string: the QC table name.
#' @export
sn_qc_table_name <- function(site, view) {
  # The "outlet"/"trib" underscore insertion mirrors real QC table names
  # (confirmed against api/sn/tables/list 2026-09-24): e.g. site
  # "W50_SalmTrib11_T1" -> table "w50_salm_trib11_t1_1hour", not
  # "w50_salmtrib11_t1_1hour". Without it, every W50_*Trib*/W50_*Outlet*
  # site (~200+ of them) resolves to a table name that doesn't exist --
  # sn_read_qc()/sn_post_qc() would silently miss real QC history instead
  # of erroring, since sn_read_qc() treats an unreachable table as "no
  # rows", not a failure.
  glue("{site}:{view}") %>%
    str_replace_all(regex("(outlet|trib)", ignore_case = TRUE), "_\\1") %>%
    tolower() %>%
    str_remove(".*/") %>%
    str_remove("samples.*$") %>%
    str_replace_all(":", "_")
}


#' Read manual QC flags for a station's underlying table
#'
#' Pulls from `sn/qc/:tableName`. One table can hold QC history for several
#' `measurement_name` values -- filter with [sn_join_qc()], not here.
#'
#' @param client A client from [sn_connect()].
#' @param table_name From [sn_qc_table_name()].
#' @param start_date,end_date Date range as `"YYYY-MM-DD"` strings.
#' @return A tibble with (at least) `measurement_name`, `measurement_time`,
#'   `quality_level`, `qc_flag`. Empty tibble with those columns if the
#'   table can't be reached.
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
#' Merges only the `measurement_name`s explicitly listed in `qc_map` -- a
#' component with no entry is left unmerged on purpose.
#'
#' @param values A tibble from [sn_read_values()].
#' @param qc_flags A tibble from [sn_read_qc()].
#' @param qc_map Named character vector: `component = measurement_name`.
#' @return `values` with `quality_level`/`qc_flag` columns added.
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
#' The main API entry point -- `site` accepts a vector so one call can cover
#' several stations.
#'
#' @param client A client from [sn_connect()].
#' @param site Character vector of site codes.
#' @param view Sample view name (recycled across all sites).
#' @param components Character vector of component names (recycled).
#' @param qc_map Named character vector, `component = measurement_name`.
#' @param start_date,end_date Date range as `"YYYY-MM-DD"` strings.
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


# #############################################################################
# B. stage_* -- local raw CSV loader (offline / reprocessing fallback)
# #############################################################################
# The Hakai network archives each sensor generation as a CSV under
# 01_raw/<STATION>/. This block reads one of those files into the SAME long
# shape sn_read_values() returns (measurement_time, site, variable, value),
# so stage_qc_pipeline.R never has to know which source it came from.

STAGE_RAW_HEADER_ROWS <- 4    # units / site / variable-code rows to skip
STAGE_RAW_TZ          <- "Etc/GMT+8"   # PST, no DST

STAGE_RAW_COLS <- c(
  "timestamp", "year", "month", "water_year",
  "stage_inst", "stage_avg", "stage_min", "stage_max", "stage_sd"
)


#' Read one raw sensor CSV into the long sn_read_values() shape
#'
#' @param sensor_id File stem of the raw CSV, e.g. `"ssn703_a"` (the file is
#'   `<raw_dir>/<sensor_id>.csv`).
#' @param raw_dir Directory holding the raw CSVs.
#' @param variable Which burst statistic to return as `value`. Default
#'   `"stage_avg"` -- the 5-min average, the primary QC value.
#' @param site_label What to put in the returned `site` column. Defaults to
#'   `sensor_id`.
#' @return A long tibble: `measurement_time`, `site`, `variable`, `value`.
#'   Empty tibble with those columns if the file is missing (with a message,
#'   not an error -- mirrors sn_read_values()).
#' @export
stage_read_raw_csv <- function(sensor_id, raw_dir, variable = "stage_avg",
                                site_label = sensor_id) {
  file_path <- file.path(raw_dir, paste0(sensor_id, ".csv"))

  if (!file.exists(file_path)) {
    message(glue("No raw CSV at {file_path} -- returning an empty tibble."))
    return(tibble(measurement_time = as.POSIXct(character()), site = character(),
                   variable = character(), value = numeric()))
  }

  raw <- read_csv(
    file_path,
    skip      = STAGE_RAW_HEADER_ROWS,
    col_names = STAGE_RAW_COLS,
    col_types = cols(
      timestamp  = col_character(),
      year       = col_integer(),
      month      = col_character(),
      water_year = col_character(),
      stage_inst = col_double(),
      stage_avg  = col_double(),
      stage_min  = col_double(),
      stage_max  = col_double(),
      stage_sd   = col_double()
    ),
    na = c("", "NA", "NaN")
  )

  if (!variable %in% names(raw)) {
    stop("variable '", variable, "' not in raw file columns: ",
         paste(names(raw), collapse = ", "))
  }

  raw %>%
    transmute(
      measurement_time = ymd_hms(timestamp, tz = STAGE_RAW_TZ),
      site             = site_label,
      variable         = variable,
      value            = .data[[variable]]
    ) %>%
    arrange(measurement_time)
}


#' Read the sensor registry, one row per sensor_id
#'
#' Thin typed wrapper so callers get consistent column names for the join in
#' run_stage_qc_*.R.
#'
#' @param registry_path Path to `sensor_registry.csv`.
#' @param station Optional station_id to filter to.
#' @return A tibble keyed on `site_id` (renamed from `sensor_id`).
#' @export
stage_read_registry <- function(registry_path, station = NULL) {
  reg <- read_csv(registry_path, show_col_types = FALSE) %>%
    rename(site_id = sensor_id)
  if (!is.null(station)) reg <- reg %>% filter(station_id == station)
  reg
}


# #############################################################################
# C. qc_* -- pre-threshold diagnostics
# #############################################################################
# Run these BEFORE picking QC thresholds (spike rate, gap-size cutoffs).

#' Summarise every gap in a raw series
#'
#' Contiguous runs of `NA` in the value column, one row per gap.
#'
#' @param data A tibble with a measurement_time and value column.
#' @param measurement_time,value Bare (unquoted) column names.
#' @return A tibble: `start`, `end`, `duration_mins`, `n_obs`.
#' @export
qc_summarise_gaps <- function(data, measurement_time = measurement_time, value = value) {
  data |>
    transmute(measurement_time = {{ measurement_time }}, value = {{ value }}) |>
    arrange(measurement_time) |>
    mutate(
      is_gap = is.na(value),
      gap_id = cumsum(is_gap & !lag(is_gap, default = FALSE))
    ) |>
    filter(is_gap) |>
    group_by(gap_id) |>
    summarise(
      start = min(measurement_time),
      end   = max(measurement_time),
      n_obs = n(),
      .groups = "drop"
    ) |>
    mutate(duration_mins = as.numeric(difftime(end, start, units = "mins"))) |>
    select(start, end, duration_mins, n_obs) |>
    arrange(desc(duration_mins))
}


#' Bin gap durations into a readable count table
#'
#' @param gaps A tibble from [qc_summarise_gaps()].
#' @param breaks_mins Bin edges in minutes.
#' @return A tibble: `bin`, `n_gaps`, `total_mins`.
#' @export
qc_gap_length_table <- function(gaps, breaks_mins = c(0, 15, 60, 180, 360, 720, 1440, 4320, Inf)) {
  gaps |>
    mutate(bin = cut(duration_mins, breaks = breaks_mins, right = FALSE,
                      dig.lab = 5)) |>
    group_by(bin) |>
    summarise(n_gaps = n(), total_mins = sum(duration_mins), .groups = "drop")
}


#' Quantiles of point-to-point rate of change
#'
#' Time-weighted (change per hour, not per row) so it's comparable across
#' any sampling interval.
#'
#' @param data A tibble with a measurement_time and value column.
#' @param measurement_time,value Bare (unquoted) column names.
#' @param probs Quantiles to report.
#' @return A named numeric vector, units of `value` per hour.
#' @export
qc_rate_of_change_quantiles <- function(data, measurement_time = measurement_time, value = value,
                                         probs = c(0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1)) {
  df <- data |>
    transmute(measurement_time = {{ measurement_time }}, value = {{ value }}) |>
    arrange(measurement_time) |>
    mutate(
      dt_hours = as.numeric(difftime(measurement_time, lag(measurement_time), units = "hours")),
      roc      = abs(value - lag(value)) / dt_hours
    )

  quantile(df$roc, probs = probs, na.rm = TRUE)
}


#' Quick histogram of rate of change
#'
#' @inheritParams qc_rate_of_change_quantiles
#' @param roc_max Optional upper x-axis cutoff.
#' @return A ggplot object.
#' @export
qc_plot_roc_histogram <- function(data, measurement_time = measurement_time, value = value, roc_max = NULL) {
  df <- data |>
    transmute(measurement_time = {{ measurement_time }}, value = {{ value }}) |>
    arrange(measurement_time) |>
    mutate(
      dt_hours = as.numeric(difftime(measurement_time, lag(measurement_time), units = "hours")),
      roc      = abs(value - lag(value)) / dt_hours
    )

  p <- ggplot(df, aes(x = roc)) +
    geom_histogram(bins = 100, fill = "steelblue") +
    labs(x = "|Δvalue| per hour", y = "count")

  if (!is.null(roc_max)) p <- p + coord_cartesian(xlim = c(0, roc_max))
  p
}


# #############################################################################
# D. qc_plot_diagnostic() / qc_plot_overlap() -- pre-flagging visual triage
# #############################################################################

#' Diagnostic plot for stage QC triage
#'
#' Three stacked, x-aligned panels: raw stage with existing QC flags,
#' rate-of-change, and an optional rolling min/max/std drift band.
#'
#' @param data A tibble with at least a measurement_time and a stage value.
#' @param measurement_time,value Bare (unquoted) column names.
#' @param flag Bare column name of an existing QC flag, or `NULL`.
#' @param roll_window Rolling window width in observations. Default `12`.
#' @param show_rolling Include the rolling panel? Default `TRUE`.
#' @param transition_dates Optional vector/tibble of dates to mark.
#' @param title Optional plot title.
#' @param value_label What the value axis is measuring, e.g. `"RH"`,
#'   `"Air Temp"`, `"PLS_Lvl"` -- used for the main panel's y-axis and the
#'   rate-of-change panel's (`"Δ{value_label} / hr"`). Default `"Stage"`
#'   keeps this stage-only unless a caller says otherwise (the generic QC
#'   scripts pass the resolved component name here so RH/temperature/etc
#'   plots don't say "Stage").
#' @return A `patchwork` object.
#' @export
qc_plot_diagnostic <- function(data,
                                measurement_time = measurement_time,
                                value = stage,
                                flag = NULL,
                                roll_window = 12,
                                show_rolling = TRUE,
                                transition_dates = NULL,
                                title = NULL,
                                value_label = "Stage") {

  flag_quo <- rlang::enquo(flag)
  has_flag <- !rlang::quo_is_null(flag_quo)

  if (!has_flag && "qc_flag" %in% names(data)) {
    message("No `flag` argument given -- using existing `qc_flag` column found in data.")
    flag_quo <- rlang::quo(qc_flag)
    has_flag <- TRUE
  }

  df <- data %>%
    transmute(
      measurement_time = {{ measurement_time }},
      value = {{ value }},
      flag  = if (has_flag) !!flag_quo else NA_character_
    ) %>%
    arrange(measurement_time)

  df <- df %>%
    mutate(
      dt_hours = as.numeric(difftime(measurement_time, lag(measurement_time), units = "hours")),
      roc = (value - lag(value)) / dt_hours
    )

  if (show_rolling) {
    df <- df %>%
      mutate(
        roll_min = roll::roll_min(value, width = roll_window),
        roll_max = roll::roll_max(value, width = roll_window),
        roll_sd  = roll::roll_sd(value, width = roll_window)
      )
  }

  p_main <- ggplot(df, aes(x = measurement_time, y = value)) +
    geom_line(color = "grey40", linewidth = 0.3)

  if (has_flag) {
    p_main <- p_main +
      geom_point(
        data = ~ filter(.x, !is.na(flag)),
        aes(color = flag, shape = flag),
        size = 1.5
      ) +
      labs(color = "QC flag", shape = "QC flag")
  }

  p_main <- p_main + labs(x = NULL, y = value_label, title = title)

  p_roc <- ggplot(df, aes(x = measurement_time, y = roc)) +
    geom_hline(yintercept = 0, color = "grey80") +
    geom_line(color = "steelblue", linewidth = 0.3) +
    labs(x = NULL, y = glue::glue("Δ{value_label} / hr"))

  panel_list <- list(p_main, p_roc)
  heights <- c(2, 1)

  if (show_rolling) {
    p_roll <- ggplot(df, aes(x = measurement_time)) +
      geom_ribbon(aes(ymin = roll_min, ymax = roll_max), fill = "grey80", alpha = 0.5) +
      geom_line(aes(y = roll_sd), color = "darkorange", linewidth = 0.3) +
      labs(x = NULL, y = glue::glue("Rolling min/max (band),\nsd (line) -- w={roll_window}"))

    panel_list <- c(panel_list, list(p_roll))
    heights <- c(heights, 1)
  }

  panels <- patchwork::wrap_plots(panel_list, ncol = 1, heights = heights)

  if (!is.null(transition_dates)) {
    transitions <- if (is.data.frame(transition_dates)) {
      transition_dates
    } else {
      tibble(date = transition_dates, label = NA_character_)
    }

    panels <- panels & geom_vline(
      data = transitions,
      aes(xintercept = as.numeric(date)),
      linetype = "dashed", color = "firebrick", alpha = 0.6
    )
  }

  panels & patchwork::plot_layout(axes = "collect")
}


#' Overlap plot for two co-located sensor generations
#'
#' Replaces the sensor-overlap / offset-vs-stage pages of the old
#' 02_inspect_stage.R. Two stacked panels: the two raw series overlaid
#' across the overlap window, and (failing - reference) vs reference stage,
#' to eyeball whether a constant datum offset is a defensible model.
#'
#' @param data A long tibble (`measurement_time`, `site`, `value`) covering
#'   both sensors -- e.g. bound rows of two [stage_read_raw_csv()] calls or a
#'   filtered [sn_read_station()] result.
#' @param failing,reference `site` values: the older sensor and its
#'   replacement/datum reference.
#' @param overlap_start,overlap_end POSIXct bounds of the overlap window.
#' @param title Optional plot title.
#' @return A `patchwork` object.
#' @export
qc_plot_overlap <- function(data, failing, reference,
                             overlap_start = NULL, overlap_end = NULL,
                             title = NULL) {

  df <- data %>%
    filter(site %in% c(failing, reference)) %>%
    { if (!is.null(overlap_start)) filter(., measurement_time >= overlap_start) else . } %>%
    { if (!is.null(overlap_end))   filter(., measurement_time <= overlap_end)   else . }

  p_series <- ggplot(df, aes(measurement_time, value, colour = site)) +
    geom_line(linewidth = 0.3, na.rm = TRUE, alpha = 0.85) +
    labs(x = NULL, y = "Stage", colour = NULL, title = title) +
    theme(legend.position = "bottom")

  wide <- df %>%
    select(measurement_time, site, value) %>%
    pivot_wider(names_from = site, values_from = value) %>%
    filter(!is.na(.data[[failing]]), !is.na(.data[[reference]])) %>%
    mutate(offset = .data[[failing]] - .data[[reference]])

  med_offset <- median(wide$offset, na.rm = TRUE)

  p_offset <- ggplot(wide, aes(.data[[reference]], offset)) +
    geom_point(size = 0.5, alpha = 0.35, colour = "steelblue") +
    geom_hline(yintercept = 0, colour = "black", linewidth = 0.4) +
    geom_hline(yintercept = med_offset, colour = "firebrick", linetype = "dashed") +
    annotate("text", x = Inf, y = med_offset, hjust = 1.05, vjust = -0.5, size = 3,
             colour = "firebrick",
             label = glue("median offset ({failing} - {reference}) = {round(med_offset, 4)}")) +
    labs(x = glue("{reference} stage (reference)"),
         y = glue("{failing} - {reference}"))

  patchwork::wrap_plots(list(p_series, p_offset), ncol = 1, heights = c(1, 1))
}


# #############################################################################
# E. qc_* -- post-flagging review & verification
# #############################################################################

#' Identify contiguous adjusted time chunks
#'
#' Collapses consecutive rows sharing the same non-"raw" flag into one row
#' per adjustment period.
#'
#' @param data A tibble with measurement_time and flag columns.
#' @param measurement_time,flag Bare (unquoted) column names.
#' @param raw_value The flag value meaning "untouched". Default `"raw"`.
#' @return A tibble: `flag`, `start`, `end`, `duration_mins`, `n_obs`.
#' @export
qc_summarise_chunks <- function(data, measurement_time = measurement_time, flag = qc_flag, raw_value = "raw") {
  data |>
    transmute(measurement_time = {{ measurement_time }}, flag = {{ flag }}) |>
    arrange(measurement_time) |>
    mutate(
      is_adjusted = flag != raw_value,
      chunk_id = cumsum(
        is_adjusted & (flag != lag(flag, default = first(flag)) | !lag(is_adjusted, default = FALSE))
      )
    ) |>
    filter(is_adjusted) |>
    group_by(chunk_id, flag) |>
    summarise(
      start = min(measurement_time),
      end   = max(measurement_time),
      n_obs = n(),
      .groups = "drop"
    ) |>
    mutate(duration_mins = as.numeric(difftime(end, start, units = "mins"))) |>
    select(flag, start, end, duration_mins, n_obs) |>
    arrange(start)
}


#' Full-record QC review plot
#'
#' Raw values as a thin grey line, QC'd values in colour only where they
#' differ, adjusted chunks shaded.
#'
#' @param data A tibble with measurement_time, raw value, QC'd value, flag.
#' @param measurement_time,value,value_qc,flag Bare (unquoted) column names.
#' @param raw_value The flag value meaning "untouched". Default `"raw"`.
#' @param chunks Optional pre-computed [qc_summarise_chunks()] table.
#' @return A ggplot object.
#' @export
qc_plot_review <- function(data, measurement_time = measurement_time, value = value,
                            value_qc = value_qc, flag = qc_flag,
                            raw_value = "raw", chunks = NULL) {

  df <- data |>
    transmute(
      measurement_time = {{ measurement_time }},
      value     = {{ value }},
      value_qc  = {{ value_qc }},
      flag      = {{ flag }}
    ) |>
    arrange(measurement_time)

  if (is.null(chunks)) {
    chunks <- qc_summarise_chunks(df, measurement_time, flag, raw_value)
  }

  p <- ggplot(df, aes(x = measurement_time))

  if (nrow(chunks) > 0) {
    p <- p + geom_rect(
      data = chunks,
      aes(xmin = start, xmax = end, ymin = -Inf, ymax = Inf, fill = flag),
      inherit.aes = FALSE, alpha = 0.15
    )
  }

  p +
    geom_line(aes(y = value), color = "grey50", linewidth = 0.3) +
    geom_line(
      data = ~ filter(.x, flag != raw_value),
      aes(y = value_qc, color = flag, group = 1),
      linewidth = 0.5
    ) +
    labs(
      x = NULL, y = NULL, fill = "Adjusted chunk", color = "Adjusted chunk",
      subtitle = "Grey = raw value | Coloured = QC'd value, shown only where flag != raw"
    ) +
    theme(legend.position = "bottom")
}


#' Zoom into one QC chunk for close verification
#'
#' @param data As in [qc_plot_review()].
#' @param measurement_time,value,value_qc,flag Bare (unquoted) column names.
#' @param chunk A single row from [qc_summarise_chunks()].
#' @param buffer_hours Context on either side. Default `6`.
#' @return A ggplot object.
#' @export
qc_plot_chunk <- function(data, measurement_time = measurement_time, value = value,
                           value_qc = value_qc, flag = qc_flag,
                           chunk, buffer_hours = 6) {

  window_start <- chunk$start - lubridate::hours(buffer_hours)
  window_end   <- chunk$end   + lubridate::hours(buffer_hours)

  df <- data |>
    transmute(
      measurement_time = {{ measurement_time }},
      value     = {{ value }},
      value_qc  = {{ value_qc }},
      flag      = {{ flag }}
    ) |>
    filter(measurement_time >= window_start, measurement_time <= window_end) |>
    arrange(measurement_time)

  ggplot(df, aes(x = measurement_time)) +
    annotate("rect", xmin = chunk$start, xmax = chunk$end, ymin = -Inf, ymax = Inf,
             fill = "steelblue", alpha = 0.15) +
    geom_line(aes(y = value), color = "grey50", linewidth = 0.4) +
    geom_point(aes(y = value), color = "grey50", size = 0.8) +
    geom_line(aes(y = value_qc), color = "firebrick", linewidth = 0.4) +
    geom_point(aes(y = value_qc), color = "firebrick", size = 0.8) +
    labs(
      x = NULL, y = NULL,
      title = glue::glue("{chunk$flag}: {format(chunk$start)} to {format(chunk$end)} ({chunk$duration_mins} min)"),
      subtitle = "Grey = raw | Red = QC'd | Shaded band = the adjusted chunk itself"
    )
}


#' Interactive QC plot (WebGL, performance-focused)
#'
#' Like [qc_plot_review()] but interactive via plotly. Built with
#' `plot_ly()` directly (not `ggplotly()`) and `scattergl`, with hover
#' disabled on the raw line, so it stays responsive on multi-year 5-minute
#' records.
#'
#' @param data A tibble with measurement_time, raw value, QC'd value, flag.
#' @param measurement_time,value,value_qc,flag Bare (unquoted) column names.
#' @param raw_value The flag value meaning "untouched". Default `"raw"`.
#' @return A `plotly` object.
#' @export
qc_plot_interactive <- function(data, measurement_time = measurement_time, value = value,
                                 value_qc = value_qc, flag = qc_flag, raw_value = "raw") {

  df <- data |>
    transmute(
      measurement_time = {{ measurement_time }},
      value    = {{ value }},
      value_qc = {{ value_qc }},
      flag     = {{ flag }}
    ) |>
    arrange(measurement_time)

  flagged <- df |> filter(flag != raw_value)

  plotly::plot_ly() |>
    plotly::add_trace(
      data = df, x = ~measurement_time, y = ~value,
      type = "scattergl", mode = "lines",
      line = list(color = "grey", width = 1),
      name = "raw", hoverinfo = "skip"
    ) |>
    plotly::add_trace(
      data = flagged, x = ~measurement_time, y = ~value_qc,
      type = "scattergl", mode = "markers", color = ~flag,
      marker = list(size = 5),
      text = ~paste0("Flag: ", flag,
                      "<br>Time: ", format(measurement_time),
                      "<br>Value: ", round(value_qc, 4)),
      hoverinfo = "text"
    ) |>
    plotly::layout(
      xaxis = list(title = ""),
      yaxis = list(title = ""),
      legend = list(title = list(text = "QC flag"))
    )
}


#' Verify gap-fill flags line up with real transmission gaps
#'
#' @param pls_raw Raw values tibble (before QC), measurement_time/value.
#' @param pls_qc QC'd tibble (after the pipeline), measurement_time/qc_flag.
#' @param measurement_time,value,qc_flag Bare (unquoted) column names.
#' @param fill_flags Which `qc_flag` values count as gap-fill.
#' @return A list: `gap_audit` (one row per real gap) and `mismatches`
#'   (fill-flagged points NOT inside a real gap -- normally empty).
#' @export
qc_verify_gap_fill <- function(pls_raw, pls_qc, measurement_time = measurement_time,
                                value = value, qc_flag = qc_flag,
                                fill_flags = c("gf_spline", "gf_sa")) {
  raw_df <- pls_raw |> transmute(measurement_time = {{ measurement_time }}, value = {{ value }})
  qc_df  <- pls_qc  |> transmute(measurement_time = {{ measurement_time }}, qc_flag = {{ qc_flag }})

  gaps <- qc_summarise_gaps(raw_df, measurement_time, value)

  gap_audit <- gaps |>
    rowwise() |>
    mutate(
      filled_as = qc_df |>
        filter(measurement_time >= start, measurement_time <= end) |>
        pull(qc_flag) |>
        unique() |>
        paste(collapse = ", ")
    ) |>
    ungroup()

  fill_points <- qc_df |> filter(qc_flag %in% fill_flags)

  fill_points <- fill_points |>
    rowwise() |>
    mutate(inside_known_gap = nrow(gaps) > 0 &&
             any(measurement_time >= gaps$start & measurement_time <= gaps$end)) |>
    ungroup()

  mismatches <- fill_points |> filter(!inside_known_gap)

  if (nrow(mismatches) > 0) {
    warning(glue(
      "{nrow(mismatches)} rows flagged as gap-filled ({paste(fill_flags, collapse = '/')}) ",
      "do NOT fall inside a real transmission gap -- worth investigating."
    ))
  } else {
    message("All ", nrow(fill_points), " gap-filled points fall inside a real transmission gap. Looks correct.")
  }

  list(gap_audit = gap_audit, mismatches = mismatches)
}


#' Compare summary statistics before and after QC
#'
#' @param data A tibble with a raw value and a QC'd value column.
#' @param value,value_qc Bare (unquoted) column names.
#' @return A tibble: one row per statistic, columns `raw`, `qc`, `diff`.
#' @export
qc_compare_summary <- function(data, value = value, value_qc = value_qc) {
  df <- data |> transmute(value = {{ value }}, value_qc = {{ value_qc }})

  summarise_one <- function(x) {
    tibble(
      n         = length(x),
      n_missing = sum(is.na(x)),
      mean      = mean(x, na.rm = TRUE),
      median    = median(x, na.rm = TRUE),
      sd        = sd(x, na.rm = TRUE),
      min       = min(x, na.rm = TRUE),
      max       = max(x, na.rm = TRUE)
    )
  }

  bind_rows(raw = summarise_one(df$value), qc = summarise_one(df$value_qc), .id = "series") |>
    pivot_longer(-series, names_to = "stat", values_to = "value") |>
    pivot_wider(names_from = series, values_from = value) |>
    mutate(diff = if_else(stat %in% c("n", "n_missing"), NA_real_, qc - raw))
}


#' Check for unexpected missing values in the QC'd series
#'
#' A QC'd value should only be `NA` under an `unfilled_*` flag. Anything
#' else means the pipeline is silently dropping a value.
#'
#' @param data A tibble with a QC'd value and a flag column.
#' @param value_qc,qc_flag Bare (unquoted) column names.
#' @return A tibble: one row per `qc_flag`, with `n` and `n_missing`.
#' @export
qc_check_missing <- function(data, value_qc = value_qc, qc_flag = qc_flag) {
  df <- data |> transmute(value_qc = {{ value_qc }}, qc_flag = {{ qc_flag }})

  by_flag <- df |>
    group_by(qc_flag) |>
    summarise(n = n(), n_missing = sum(is.na(value_qc)), .groups = "drop")

  unexpected <- by_flag |>
    filter(n_missing > 0, !str_starts(qc_flag, "unfilled"))

  if (nrow(unexpected) > 0) {
    warning(glue(
      "Unexpected NA in value_qc for non-unfilled flag(s): ",
      "{paste(unexpected$qc_flag, collapse = ', ')} -- worth investigating."
    ))
  } else {
    message("No unexpected missing values -- every NA in value_qc falls under an 'unfilled_*' flag, as expected.")
  }

  by_flag
}
