# ============================================================
# qc_plot_diagnostic() -- visual QC triage for stage data
# ============================================================
# Draft diagnostic plotting function, meant to sit ahead of any
# flag-assignment logic (spike / drift / ice_affected /
# sensor_transition / gap) in a hydrocan-style QC pipeline.
#
# Install patchwork if you don't have it: install.packages("patchwork")

library(tidyverse)
library(roll)
library(patchwork)


#' Diagnostic plot for stage QC triage
#'
#' Three stacked, x-aligned panels: raw stage with existing QC
#' flags overlaid (if present), rate-of-change to make spikes/jumps
#' obvious, and an optional rolling min/max/std band to catch slow
#' drift. Meant for visual inspection before deciding on flag logic
#' -- not itself a QC/flagging function.
#'
#' @param data A dataframe/tibble with at least a measurement_time and a
#'   stage value column.
#' @param measurement_time,value Bare (unquoted) column names for the
#'   measurement_time and stage value, e.g. `measurement_time = date`,
#'   `value = stage`. Defaults assume columns literally named
#'   `measurement_time` and `stage`.
#' @param flag Bare column name holding an existing QC flag, if any
#'   (e.g. `qc_flag`). Optional -- leave as `NULL` (the default) to
#'   skip overlay; if left `NULL` and a column literally named
#'   `qc_flag` exists in `data`, it's used automatically with a
#'   message telling you it did so.
#' @param roll_window Rolling window width, in number of
#'   observations (not time), for the drift band. Default `12` --
#'   e.g. 1 hour of 5-minute data. Adjust to your data's interval.
#' @param show_rolling Include the rolling min/max/std panel?
#'   Default `TRUE`.
#' @param transition_dates Optional vector of dates/datetimes (or a
#'   tibble with `date` and `label` columns) to mark with vertical
#'   reference lines -- e.g. sensor generation swap dates, so you
#'   can see whether discontinuities line up with them.
#' @param title Optional plot title, e.g. a station code.
#' @return A `patchwork` object (stacked ggplots) -- print it, or
#'   keep composing with `+`/`/` like any patchwork.
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

  # Standardize to working column names internally, keeping the
  # tidy-eval interface for the caller.
  df <- data %>%
    transmute(
      measurement_time = {{ measurement_time }},
      value     = {{ value }},
      flag      = if (has_flag) !!flag_quo else NA_character_
    ) %>%
    arrange(measurement_time)

  # ---- Rate of change -------------------------------------------------------
  # Time-weighted so it's meaningful even across gaps/irregular sampling.
  df <- df %>%
    mutate(
      dt_hours = as.numeric(difftime(measurement_time, lag(measurement_time), units = "hours")),
      roc = (value - lag(value)) / dt_hours
    )

  # ---- Rolling min/max/std ---------------------------------------------------
  if (show_rolling) {
    df <- df %>%
      mutate(
        roll_min = roll::roll_min(value, width = roll_window),
        roll_max = roll::roll_max(value, width = roll_window),
        roll_sd  = roll::roll_sd(value, width = roll_window)
      )
  }

  # ---- Panel 1: raw stage + flags --------------------------------------------
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

  # ---- Panel 2: rate of change ------------------------------------------------
  p_roc <- ggplot(df, aes(x = measurement_time, y = roc)) +
    geom_hline(yintercept = 0, color = "grey80") +
    geom_line(color = "steelblue", linewidth = 0.3) +
    labs(x = NULL, y = "\u0394stage / hr")

  panel_list <- list(p_main, p_roc)
  heights <- c(2, 1)

  # ---- Panel 3: rolling drift band --------------------------------------------
  if (show_rolling) {
    p_roll <- ggplot(df, aes(x = measurement_time)) +
      geom_ribbon(aes(ymin = roll_min, ymax = roll_max), fill = "grey80", alpha = 0.5) +
      geom_line(aes(y = roll_sd), color = "darkorange", linewidth = 0.3) +
      labs(x = NULL, y = glue::glue("Rolling min/max (band),\nsd (line) -- w={roll_window}"))

    panel_list <- c(panel_list, list(p_roll))
    heights <- c(heights, 1)
  }

  # Combine via wrap_plots() rather than the `/` operator -- recent ggplot2
  # (S7-based plot objects) can intercept `/` before patchwork's method
  # resolves, depending on installed versions. wrap_plots() is a direct
  # function call and avoids that ambiguity.
  panels <- patchwork::wrap_plots(panel_list, ncol = 1, heights = heights)

  # ---- Sensor transition markers, if given -------------------------------------
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


# ================================================================
# Example
# ================================================================
# qc_plot_diagnostic(
#   ssn703_data,                 # already has measurement_time, e.g. from sn_read_values()
#   value     = value,          # or `stage`, whatever your column is called
#   flag      = qc_flag,        # omit entirely to auto-detect/skip
#   roll_window = 12,
#   transition_dates = tibble(
#     date  = as.Date(c("2019-05-28", "2021-03-25", "2021-09-02")),
#     label = c("PLS2 temp end", "PLS2 level end", "PLS4 start")
#   ),
#   title = "SSN703US"
# )
# # measurement_time is still remappable if your data calls it something else:
# # qc_plot_diagnostic(some_data, measurement_time = date, value = stage)
