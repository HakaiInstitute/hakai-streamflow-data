# ============================================================
# qc_* diagnostics -- run BEFORE picking QC thresholds
# ============================================================
# Two questions to answer from real data before touching
# SPIKE_RATE_PER_5MIN or MIN_GAP_FOR_SA_MINS in
# pls_qc_for_upload_example.R:
#   1. How many gaps are there, and how long are they? Where's
#      the natural break between "small, spline handles it fine"
#      and "large, worth trying SA for"?
#   2. How much does the raw signal actually move point-to-point
#      in a normal 5-minute step? Where's the natural break between
#      "real hydrological variability" and "spike"?
#
# Both return plain tibbles/vectors you can print, filter, or plot
# however's useful -- this isn't meant to pick thresholds for you,
# just to make the real distribution visible so you can.

library(tidyverse)


#' Summarise every gap in a raw series
#'
#' Identifies contiguous runs of `NA` in the raw value column and
#' reports each one's duration -- the same gap-detection logic used
#' internally by the QC pipeline, exposed here on its own so you can
#' inspect it before any QC runs.
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
#' @param breaks_mins Bin edges in minutes. Default covers 15 min to
#'   2+ days -- adjust to whatever granularity is useful for this
#'   station.
#' @return A tibble: `bin`, `n_gaps`, `total_mins` (sum of duration
#'   in that bin -- useful for seeing which bin actually dominates
#'   the record, not just which has the most individual gaps).
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
#' Time-weighted so it's meaningful across any sampling interval:
#' change per hour, not per row. Use this to see where "normal"
#' variability ends and where a spike threshold might actually make
#' sense, instead of reusing a threshold from a different station or
#' variable.
#'
#' @param data A tibble with a measurement_time and value column.
#' @param measurement_time,value Bare (unquoted) column names.
#' @param probs Quantiles to report. Default covers the middle of
#'   the distribution up through the extreme tail.
#' @return A named numeric vector, one entry per requested quantile,
#'   in the same units as `value` per hour.
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
#' @param roc_max Optional upper x-axis cutoff, to zoom past the
#'   handful of extreme outliers that otherwise crush the rest of
#'   the histogram into one bar. Leave `NULL` to show everything.
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


# ================================================================
# Example workflow
# ================================================================
# # 1. Gaps -- how many, how long, where's the natural break?
 gaps <- qc_summarise_gaps(pls_raw, measurement_time, value)
 print(gaps, n = Inf)                 # every individual gap
 qc_gap_length_table(gaps)            # binned counts + total time
#
# # 2. Rate of change -- where does "normal" end and "spike" begin?
 qc_rate_of_change_quantiles(pls_raw, measurement_time, value)
 qc_plot_roc_histogram(pls_raw, measurement_time, value)
 qc_plot_roc_histogram(pls_raw, measurement_time, value, roc_max = 2)  # zoomed in
#
# # 3. Once you've picked thresholds from the above, set them in
# # pls_qc_for_upload_example.R:
# #   SPIKE_RATE_PER_5MIN <- <your chosen value, converted from per-hour if needed>
# #   MIN_GAP_FOR_SA_MINS <- <your chosen value>