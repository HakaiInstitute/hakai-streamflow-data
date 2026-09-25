# ============================================================
# QC review tools -- verify adjusted time chunks visually
# ============================================================
# Four pieces meant to be used together:
#   qc_summarise_chunks()  -- table of every contiguous adjusted period
#   qc_plot_review()       -- full-record overview (ggplot), chunks shaded
#   qc_plot_chunk()         -- zoom into one chunk, raw vs QC'd overlaid
#   qc_plot_interactive()  -- interactive, WebGL-based version of the
#                              full-record overview -- zoom/pan/hover
#
# Works on the output of pls_qc_for_upload_example.R (or any QC pipeline
# with the same shape: a measurement_time, a raw value column, a QC'd value
# column, and a categorical flag column).

library(tidyverse)
library(plotly)


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
#' adjusted chunks shaded in the background so you can see at a glance
#' where and how much was touched.
#'
#' @param data A tibble with measurement_time, raw value, QC'd value, and flag
#'   columns.
#' @param measurement_time,value,value_qc,flag Bare (unquoted) column names.
#' @param raw_value The flag value meaning "untouched". Default `"raw"`.
#' @param chunks Optional pre-computed chunk table from [qc_summarise_chunks()]
#'   (computed automatically if not supplied -- pass it in if you're
#'   calling this repeatedly, to avoid recomputing).
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
#' Shows raw vs QC'd value for a single adjustment period plus a buffer
#' on either side, so you can check the correction actually did what you
#' expected against the surrounding context.
#'
#' @param data As in [qc_plot_review()].
#' @param measurement_time,value,value_qc,flag Bare (unquoted) column names.
#' @param chunk A single row from [qc_summarise_chunks()] (e.g. `chunks[1, ]`
#'   or `chunks |> filter(flag == "gf_sa") |> slice(1)`).
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
#' Like [qc_plot_review()] but interactive via plotly -- zoom, pan, and
#' hover for exact values/timestamps/flags. Built directly with
#' `plot_ly()` rather than `ggplotly()`, since converting a ggplot tends
#' to bog down on long 5-minute-interval records. Two performance
#' choices worth knowing about:
#'   - Uses `scattergl` (WebGL) rather than plotly's default SVG
#'     rendering, which matters once you're past a few thousand points.
#'   - The raw line has hover disabled entirely (`hoverinfo = "skip"`) --
#'     hover-target computation, not rendering, is usually the actual
#'     bottleneck on a long series. Only the flagged points (normally a
#'     small fraction of the record) get hover text.
#'
#' @param data A tibble with measurement_time, raw value, QC'd value,
#'   and flag columns.
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


# ================================================================
# Example workflow
# ================================================================
# chunks <- qc_summarise_chunks(pls, measurement_time, qc_flag)
# print(chunks, n = Inf)
#
# # Full-record overview:
# qc_plot_review(pls, measurement_time, value, value_qc, qc_flag, chunks = chunks)
#
# # Zoom into a specific chunk to verify it -- e.g. the largest SA-filled gap:
# biggest_sa_gap <- chunks |> filter(flag == "gf_sa") |> slice_max(duration_mins, n = 1)
# qc_plot_chunk(pls, measurement_time, value, value_qc, qc_flag, chunk = biggest_sa_gap)
#
# # Or step through every chunk of a given type one at a time:
# spike_chunks <- chunks |> filter(flag == "spike")
# for (i in seq_len(nrow(spike_chunks))) {
#   print(qc_plot_chunk(pls, measurement_time, value, value_qc, qc_flag, chunk = spike_chunks[i, ]))
# }
#
# # Interactive version -- zoom/pan/hover over the whole record:
# qc_plot_interactive(pls, measurement_time, value, value_qc, qc_flag)