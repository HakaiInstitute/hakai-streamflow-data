# =============================================================================
# qc_functions.R -- everything reusable for the SSN703 PLS QC workflow
# =============================================================================
# Source this ONE file, then run pls_qc_workflow.R.
#
# Function naming modeled on hydrocan/tidyhydat: <prefix>_<verb>_<noun>(),
# snake_case throughout, tibble in -> tibble out. `sn_` = Hakai Sensor
# Network API access; `qc_` = quality control (diagnostics, flagging
# support, review). measurement_time is the one column name used
# end-to-end everywhere in this file -- see the note under section A.
#
# Sections:
#   A. sn_*  -- API download (sn_connect, sn_read_values, sn_read_qc, ...)
#   B. qc_*  -- pre-threshold diagnostics (gap lengths, rate of change)
#   C. qc_*  -- pre-flagging visual triage (qc_plot_diagnostic)
#   D. qc_*  -- post-flagging review & verification (chunks, plots,
#               qc_verify_gap_fill)
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
#'   real, non-error outcome -- e.g. a sensor not active yet/anymore for
#'   that window), returns a 0-row tibble with those same columns rather
#'   than erroring.
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
#' <https://hakaiinstitute.github.io/hakai-api/endpoints/>). One table can
#' hold QC history for several `measurement_name` values at once -- filter
#' with [sn_join_qc()], not here.
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
#' component with no entry in `qc_map` is left unmerged on purpose (e.g.
#' `PLS_Lvl`, which has no QC history under that name; only its calculated
#' derivative `Stage` does, and that's a different variable, not a
#' stand-in for it).
#'
#' @param values A tibble from [sn_read_values()].
#' @param qc_flags A tibble from [sn_read_qc()].
#' @param qc_map Named character vector: `component = measurement_name`,
#'   e.g. `c(PLS_Temp = "PLS_Temp")`. Only components named here get QC
#'   columns merged in.
#' @return `values` with `quality_level`/`qc_flag` columns added. Rows for
#'   unmapped components (or with no matching QC row) get `NA` in both --
#'   that's expected, not a merge failure.
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
#' The main entry point -- like `hc_read_daily_flows()` in hydrocan or
#' `hy_daily_flows()` in tidyhydat, `site` accepts a vector so one call can
#' cover several stations at once.
#'
#' @param client A client from [sn_connect()].
#' @param site Character vector of site codes.
#' @param view Sample view name (recycled across all sites).
#' @param components Character vector of component names (recycled across
#'   all sites).
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


# #############################################################################
# B. qc_* -- pre-threshold diagnostics
# #############################################################################
# Run these BEFORE picking QC thresholds (spike rate, gap-size cutoffs).
#' Summarise every gap in a raw series
#'
#' Identifies contiguous runs of `NA` in the raw value column and reports
#' each one's duration.
#'
#' @param data A tibble with a measurement_time and value column.
#' @param measurement_time,value Bare (unquoted) column names.
#' @return A tibble: one row per gap, with `start`, `end`,
#'   `duration_mins`, `n_obs`.
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
#' @param breaks_mins Bin edges in minutes. Default covers 15 min to 2+
#'   days -- adjust to whatever granularity is useful for this station.
#' @return A tibble: `bin`, `n_gaps`, `total_mins` (sum of duration in
#'   that bin -- useful for seeing which bin actually dominates the
#'   record, not just which has the most individual gaps).
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
#' Time-weighted so it's meaningful across any sampling interval: change
#' per hour, not per row.
#'
#' @param data A tibble with a measurement_time and value column.
#' @param measurement_time,value Bare (unquoted) column names.
#' @param probs Quantiles to report. Default covers the middle of the
#'   distribution up through the extreme tail.
#' @return A named numeric vector, one entry per requested quantile, in
#'   the same units as `value` per hour.
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


#' Quick histogram of rate of change, for a visual look
#'
#' @inheritParams qc_rate_of_change_quantiles
#' @param roc_max Optional upper x-axis cutoff, to zoom past the handful
#'   of extreme outliers that otherwise crush the rest of the histogram
#'   into one bar. Leave `NULL` to show everything.
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
    labs(x = "|\u0394value| per hour", y = "count")

  if (!is.null(roc_max)) p <- p + coord_cartesian(xlim = c(0, roc_max))
  p
}


# #############################################################################
# C. qc_plot_diagnostic() -- pre-flagging visual triage
# #############################################################################
# Meant to sit ahead of any flag-assignment logic (spike / flatline /
# range / gap) -- not itself a QC/flagging function.

#' Diagnostic plot for stage QC triage
#'
#' Three stacked, x-aligned panels: raw stage with existing QC flags
#' overlaid (if present), rate-of-change to make spikes/jumps obvious, and
#' an optional rolling min/max/std band to catch slow drift.
#'
#' @param data A dataframe/tibble with at least a measurement_time and a
#'   stage value column.
#' @param measurement_time,value Bare (unquoted) column names. Defaults
#'   assume columns literally named `measurement_time` and `stage`.
#' @param flag Bare column name holding an existing QC flag, if any (e.g.
#'   `qc_flag`). Optional -- leave `NULL` to skip overlay; if left `NULL`
#'   and a column literally named `qc_flag` exists in `data`, it's used
#'   automatically with a message telling you it did so.
#' @param roll_window Rolling window width, in number of observations
#'   (not time), for the drift band. Default `12` -- e.g. 1 hour of
#'   5-minute data.
#' @param show_rolling Include the rolling min/max/std panel? Default
#'   `TRUE`.
#' @param transition_dates Optional vector of dates/datetimes (or a
#'   tibble with `date` and `label` columns) to mark with vertical
#'   reference lines -- e.g. sensor generation swap dates.
#' @param title Optional plot title, e.g. a station code.
#' @return A `patchwork` object (stacked ggplots).
#' @export
qc_plot_diagnostic <- function(data,
                                measurement_time = measurement_time,
                                value = stage,
                                flag = NULL,
                                roll_window = 12,
                                show_rolling = TRUE,
                                transition_dates = NULL,
                                title = NULL) {

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

  p_main <- p_main + labs(x = NULL, y = "Stage", title = title)

  p_roc <- ggplot(df, aes(x = measurement_time, y = roc)) +
    geom_hline(yintercept = 0, color = "grey80") +
    geom_line(color = "steelblue", linewidth = 0.3) +
    labs(x = NULL, y = "\u0394stage / hr")

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

  # wrap_plots() rather than `/` -- recent ggplot2 (S7-based plot objects)
  # can intercept `/` before patchwork's method resolves, depending on
  # installed versions.
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


# #############################################################################
# D. qc_* -- post-flagging review & verification
# #############################################################################
# Use these AFTER running the QC pipeline, to visually and numerically
# confirm the flags/fills are doing what you expect.

#' Identify contiguous adjusted time chunks
#'
#' Collapses consecutive rows sharing the same non-"raw" flag into single
#' chunks, so you get one row per adjustment period instead of one row per
#' observation.
#'
#' @param data A tibble with measurement_time and flag columns.
#' @param measurement_time,flag Bare (unquoted) column names.
#' @param raw_value The flag value meaning "untouched" -- excluded from
#'   chunks. Default `"raw"`.
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
#' Raw values as a thin grey line, QC'd values overlaid in colour only
#' where they differ from raw (i.e. wherever a flag applied), with
#' adjusted chunks shaded in the background.
#'
#' @param data A tibble with measurement_time, raw value, QC'd value, and
#'   flag columns.
#' @param measurement_time,value,value_qc,flag Bare (unquoted) column names.
#' @param raw_value The flag value meaning "untouched". Default `"raw"`.
#' @param chunks Optional pre-computed chunk table from
#'   [qc_summarise_chunks()] (computed automatically if not supplied).
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
#' Shows raw vs QC'd value for a single adjustment period plus a buffer on
#' either side.
#'
#' @param data As in [qc_plot_review()].
#' @param measurement_time,value,value_qc,flag Bare (unquoted) column names.
#' @param chunk A single row from [qc_summarise_chunks()].
#' @param buffer_hours Context to show on either side of the chunk.
#'   Default `6`.
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
#' Like [qc_plot_review()] but interactive via plotly. Built directly with
#' `plot_ly()` rather than `ggplotly()`, since converting a ggplot tends
#' to bog down on long 5-minute-interval records. Uses `scattergl` (WebGL)
#' and disables hover on the raw line entirely, since hover-target
#' computation (not rendering) is usually the real bottleneck on a long
#' series -- only the flagged points get hover text.
#'
#' @param data A tibble with measurement_time, raw value, QC'd value, and
#'   flag columns.
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
#' Cross-checks every gap-fill-flagged point against the actual
#' transmission gaps in the raw data (via [qc_summarise_gaps()]), and
#' reports how each real gap was resolved. 
#'
#' @param pls_raw Raw values tibble (before QC), with measurement_time/value.
#' @param pls_qc QC'd tibble (after the pipeline), with measurement_time
#'   and qc_flag.
#' @param measurement_time,value,qc_flag Bare (unquoted) column names.
#' @param fill_flags Which `qc_flag` values count as "filled via gap-fill
#'   logic" and should be checked against real gaps. Default covers the
#'   standard `gf_*` names used in `pls_qc_workflow.R`.
#' @return A list with two tibbles: `gap_audit` (one row per real gap,
#'   showing how it was resolved) and `mismatches` (any fill-flagged
#'   points that do NOT fall inside a real gap -- should normally be
#'   empty; non-empty means something is being filled that isn't a real
#'   gap, worth investigating). Also prints a message/warning summarising
#'   the result.
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
#' Side-by-side stats for the raw value and the QC'd value, so you can see
#' at a glance whether QC shifted the distribution in a way that looks
#' reasonable (e.g. a slightly narrower range after spike/range removal)
#' or in a way that looks wrong (a big shift in the mean, a much wider
#' range, etc).
#'
#' @param data A tibble with a raw value and a QC'd value column.
#' @param value,value_qc Bare (unquoted) column names.
#' @return A tibble with one row per statistic (`n`, `n_missing`, `mean`,
#'   `median`, `sd`, `min`, `max`) and one column per series (`raw`,
#'   `qc`), plus a `diff` column (`qc - raw`) for the numeric stats.
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
#' A QC'd value should only be `NA` where the flag says so (an
#' `unfilled_*` flag, meaning genuinely left as MV). Any `NA` under a
#' different flag would mean something upstream in the pipeline is
#' silently dropping a value it should have filled or left untouched --
#' worth investigating, not expected behaviour.
#'
#' @param data A tibble with a QC'd value and a flag column.
#' @param value_qc,qc_flag Bare (unquoted) column names.
#' @return A tibble: one row per `qc_flag` category, with `n` (total rows)
#'   and `n_missing` (how many have `NA` in `value_qc`). Also prints a
#'   message/warning: clean if every `n_missing` outside `unfilled_*`
#'   categories is `0`, a warning otherwise naming which categories have
#'   unexpected gaps.
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


# =============================================================================
# runs
# =============================================================================
 client <- sn_connect()
#
 pls_raw <- sn_read_values(client, "SSN703US", "5minuteSamples", "PLS3_Lvl",
                            start_date = "2018-09-14", end_date = "2023-08-03")
 sa_raw  <- sn_read_values(client, "SA_WTS703_PT", "5minuteSamples", "SensorDepth_Avg",
                            start_date = "2018-09-14", end_date = "2023-08-03")
#
# # Pre-threshold diagnostics:
 gaps <- qc_summarise_gaps(pls_raw, measurement_time, value)
 qc_gap_length_table(gaps)
 qc_rate_of_change_quantiles(pls_raw, measurement_time, value)
 qc_plot_roc_histogram(pls_raw, measurement_time, value, roc_max = 2)
#
# # Run pls_qc_workflow.R using pls_raw/sa_raw, producing `pls` and `pls_for_db` ...
#
# # Post-flagging review:
# chunks <- qc_summarise_chunks(pls, measurement_time, qc_flag)
# qc_plot_review(pls, measurement_time, value, value_qc, qc_flag, chunks = chunks)
# qc_plot_interactive(pls, measurement_time, value, value_qc, qc_flag)
# qc_verify_gap_fill(pls_raw, pls, measurement_time, value, qc_flag)
# qc_compare_summary(pls, value, value_qc)
# qc_check_missing(pls, value_qc, qc_flag)