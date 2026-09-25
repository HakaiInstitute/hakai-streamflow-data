# =============================================================================
# qc_generic_pipeline.R -- the generic QC engine (any 5-min measurement)
# =============================================================================
# Source stage_qc/stage_qc_functions.R first (for the reusable qc_* diagnostic
# layer -- qc_summarise_gaps, qc_plot_diagnostic, qc_verify_gap_fill, etc.,
# already fully generic), then this.
#
# Generalised from stage_qc_pipeline.R::stage_qc_run(). That engine is
# already parameterised (offset/bad-data/force-fill windows, thresholds all
# come from a `params` list) EXCEPT for two things that are stage-specific:
#   1. its default thresholds (range_min/max, flatline_tolerance) are stage
#      numbers (metres) -- meaningless for RH (%) or temperature (deg C).
#   2. its naming ("stage", "pls", "sa"/"stage_sa") assumes water level and a
#      water-level reference sensor.
# This file keeps the exact same 7-step structure and correctness fixes
# (gap_id increments on gap STARTS, spline-flagged-but-NA/out-of-range rows
# downgraded to unfilled, window bounds handled as real instants not
# tz-mangled strings) but:
#   - has NO default numeric thresholds for range/flatline (NA = skip that
#     check entirely) -- there is no one range valid across level/RH/temp,
#     so each variable sets its own explicitly.
#   - renames the optional reference-sensor series from `sa`/`stage_sa` to
#     `reference`/`ref_value`, so it reads as "some other correlated sensor"
#     rather than assuming a water-level SA. Passing `reference = NULL` skips
#     tiers 1/1b entirely, exactly like stage_qc_run(sa = NULL) already does.
#   - keeps spike detection disabled, matching upstream's own note that it
#     still over-captures real events.
#
# stage_qc_pipeline.R / stage_qc_functions.R / run_stage_qc_ssn703.R are NOT
# modified or sourced by anything here except stage_qc_functions.R (for the
# generic diagnostic layer) -- this file is a separate, parallel engine.
#
# Returns: a tibble with
#   measurement_time, value (offset-corrected raw), value_qc, origin,
#   fill_method, offset_applied, qc_flag, value_min, value_max, value_std
# with the reference->primary reconstruction diagnostic plot attached as
#   attr(result, "ref_recon_plot")   (NULL when no bad-data window was rebuilt)
#
# value_min/value_max/value_std ride through natively -- pass them in on
# `primary` (e.g. straight from sn_read_measurement()) and they get the same
# offset/MV treatment `value` does, no separate join step needed before
# qc_format_upload(include_stats = TRUE). Omitted on input -> NA throughout.
# =============================================================================

library(zoo)       # rollmedian, na.spline, rollapply, na.approx
library(splines)   # ns()


# -----------------------------------------------------------------------------
# Default parameters -- override per variable in the calling script
# -----------------------------------------------------------------------------
#' Default QC parameters for [qc_run()]
#'
#' Unlike `stage_qc_default_params()`, `range_min`/`range_max`/
#' `flatline_tolerance` default to `NA` -- there is no threshold that's valid
#' across water level, relative humidity, and temperature, so each variable
#' must set its own explicitly (`NA` means "skip this check").
#' @export
qc_default_params <- function() {
  list(
    # -- Range check (NA = skip) --
    range_min = NA_real_,
    range_max = NA_real_,

    # -- Flatline check (flag-only; NA tolerance = skip) --
    flatline_window_days = 3,
    flatline_tolerance   = NA_real_,

    # -- Sampling interval (row <-> minutes conversions) --
    interval_min = 5,

    # -- Gap fill --
    max_fill_gap_mins    = 180,
    ref_r2_threshold     = 0.95,
    min_gap_for_ref_mins  = 60,      # reference series never used below this

    # -- Manual windows (tibbles; empty = no-op) --
    offset_windows = tibble(start = as.POSIXct(character()),
                            end   = as.POSIXct(character()),
                            offset = numeric()),
    force_fill_windows = tibble(start = as.POSIXct(character()),
                                end   = as.POSIXct(character())),
    bad_data_windows = tibble(start = as.POSIXct(character()),
                              end   = as.POSIXct(character())),

    # -- reference -> primary reconstruction (bad-data windows) --
    ref_fit_window_days = 365,  # trusted window before the bad period to fit on
    ref_fit_spline_df   = 4,    # ns() df on the reference series
    ref_max_interp_mins = 35    # max reference-series gap to interpolate onto the grid
  )
}


# -----------------------------------------------------------------------------
# Helper: normalise a window tibble's bounds, and fill an NA `end` with the
# end of the record. Identical logic to stage_qc_pipeline.R's
# .stage_qc_prep_windows() -- duplicated (not sourced) since that function is
# internal (`.`-prefixed) to the file it lives in.
# -----------------------------------------------------------------------------
.qc_prep_windows <- function(windows, measurement_time) {
  if (nrow(windows) == 0) return(windows)
  tz_data <- attr(measurement_time, "tzone")
  if (is.null(tz_data) || !nzchar(tz_data)) tz_data <- "UTC"
  as_instant <- function(x) {
    if (inherits(x, "POSIXct")) return(x)
    lubridate::force_tz(as.POSIXct(as.character(x)), tz_data)
  }
  windows |>
    mutate(
      start = as_instant(start),
      end   = as_instant(end),
      end   = dplyr::coalesce(end, max(measurement_time, na.rm = TRUE))
    )
}


# -----------------------------------------------------------------------------
# qc_run() -- the generic engine
# -----------------------------------------------------------------------------
#' Run the generic QC pipeline for one measurement series
#'
#' @param primary A tibble with `measurement_time` (POSIXct) and `value`
#'   (numeric) -- the raw series for the measurement being QC'd. Optionally
#'   also `value_min`/`value_max`/`value_std` (e.g. passed straight through
#'   from [sn_read_measurement()]) -- if present, they're carried through the
#'   same offset/MV decisions as `value` and returned alongside it, so
#'   there's no separate stats-join step before uploading with
#'   `qc_format_upload(include_stats = TRUE)`.
#' @param reference Optional tibble with `measurement_time` and `value` for
#'   some other correlated sensor at the same site (e.g. a backup sensor).
#'   `NULL` or 0-row = no reference series; gap-fill tiers 1/1b (which need
#'   one) then skip cleanly and everything else still runs.
#' @param params A list from [qc_default_params()], with any element
#'   overridden. Set `range_min`/`range_max`/`flatline_tolerance` explicitly
#'   for this variable -- they default to `NA` (check skipped) on purpose.
#' @return A tibble (see file header) with `attr(., "ref_recon_plot")`.
#' @export
qc_run <- function(primary, reference = NULL, params = qc_default_params()) {

  # NB: don't use modifyList() -- it mangles the tibble-valued params
  # (offset_windows / bad_data_windows / force_fill_windows) into broken
  # column-wise merges. Replace named elements wholesale instead.
  p <- qc_default_params()
  p[names(params)] <- params
  flatline_window_rows <- p$flatline_window_days * 24 * 60 / p$interval_min

  stat_cols <- intersect(c("value_min", "value_max", "value_std"), names(primary))
  df <- primary |> arrange(measurement_time) |> select(measurement_time, value, any_of(stat_cols))
  for (col in c("value_min", "value_max", "value_std")) {
    if (!col %in% names(df)) df[[col]] <- NA_real_
  }
  if (anyDuplicated(df$measurement_time)) {
    n_dup <- sum(duplicated(df$measurement_time))
    warning(n_dup, " duplicate timestamps in the primary series -- keeping the first of each.")
    df <- df |> distinct(measurement_time, .keep_all = TRUE)
  }

  ref <- if (is.null(reference) || nrow(reference) == 0) {
    tibble(measurement_time = as.POSIXct(character()), ref_value = numeric())
  } else {
    reference |> arrange(measurement_time) |>
      distinct(measurement_time, .keep_all = TRUE) |>   # a dup here would fan out df on the join
      transmute(measurement_time, ref_value = value)
  }

  message("qc_run: ", nrow(df), " primary rows, ", nrow(ref), " reference rows, ",
          if (length(stat_cols) > 0) {
            paste0("burst stats found (", paste(stat_cols, collapse = ", "), ") -- carried through natively")
          } else {
            "no burst stats on input (value_min/value_max/value_std will be NA throughout)"
          })


  # ---------------------------------------------------------------------------
  # 1b. Datum/calibration offset (from params$offset_windows)
  # ---------------------------------------------------------------------------
  df$offset_applied <- NA_real_
  offset_windows <- .qc_prep_windows(p$offset_windows, df$measurement_time)

  for (i in seq_len(nrow(offset_windows))) {
    in_window <- df$measurement_time >= offset_windows$start[i] &
      df$measurement_time <= offset_windows$end[i]
    df$value[in_window]           <- df$value[in_window] + offset_windows$offset[i]
    df$value_min[in_window]       <- df$value_min[in_window] + offset_windows$offset[i]
    df$value_max[in_window]       <- df$value_max[in_window] + offset_windows$offset[i]
    # value_std NOT shifted -- a constant offset moves a level, not a spread
    df$offset_applied[in_window]  <- offset_windows$offset[i]
  }

  message("Offset correction: ", sum(!is.na(df$offset_applied)), " rows adjusted across ",
          nrow(offset_windows), " window(s)")


  # ---------------------------------------------------------------------------
  # 2. Range check (skipped entirely if range_min/range_max are NA)
  # ---------------------------------------------------------------------------
  df <- df |>
    mutate(
      origin   = if_else(is.na(value), "transmission_gap", "raw"),
      value_qc = value
    )

  if (!is.na(p$range_min) && !is.na(p$range_max)) {
    df <- df |>
      mutate(
        origin   = if_else(origin == "raw" & (value_qc < p$range_min | value_qc > p$range_max),
                            "range_fail", origin),
        value_qc = if_else(origin == "range_fail", NA_real_, value_qc)
      )
    message("Range check: ", sum(df$origin == "range_fail"),
            " rows outside [", p$range_min, ", ", p$range_max, "]")
  } else {
    message("Range check: skipped (range_min/range_max not set)")
  }


  # ---------------------------------------------------------------------------
  # 3. Flatline check (flag-only -- value_qc NOT nulled; skipped if NA)
  # ---------------------------------------------------------------------------
  if (!is.na(p$flatline_tolerance)) {
    df <- df |>
      mutate(
        roll_range = zoo::rollapply(
          value_qc, width = flatline_window_rows,
          FUN = function(x) if (all(is.na(x))) NA_real_ else diff(range(x, na.rm = TRUE)),
          align = "center", fill = NA, partial = TRUE
        ),
        origin = if_else(origin == "raw" & !is.na(roll_range) & roll_range <= p$flatline_tolerance,
                          "flatline", origin)
      ) |>
      select(-roll_range)
    message("Flatline check: ", sum(df$origin == "flatline"),
            " rows flatter than ", p$flatline_tolerance, " over ",
            p$flatline_window_days, " days (flag-only, value kept)")
  } else {
    message("Flatline check: skipped (flatline_tolerance not set)")
  }


  # ---------------------------------------------------------------------------
  # 3b. Confirmed bad-data windows (manual)
  # ---------------------------------------------------------------------------
  bad_windows <- .qc_prep_windows(p$bad_data_windows, df$measurement_time)

  df$in_bad_window <- FALSE
  if (nrow(bad_windows) > 0) {
    df$in_bad_window <- purrr::reduce(
      purrr::map2(bad_windows$start, bad_windows$end,
                 ~ df$measurement_time >= .x & df$measurement_time <= .y),
      `|`
    )
    df <- df |>
      mutate(
        # Section 6 (spline) excludes ANY row inside a bad-data window -- see
        # the `in_bad_window` guard there. Interpolating a stray
        # no-transmission row in the middle of a long bad-data block sends
        # na.spline() to +/-1e10.
        origin   = if_else(in_bad_window & origin != "transmission_gap", "bad_data", origin),
        value_qc = if_else(origin == "bad_data", NA_real_, value_qc)
      )
  }
  message("Bad-data windows: ", sum(df$origin == "bad_data"),
          " rows flagged bad_data (rebuilt from the reference series in 5b where coverage allows)")


  # ---------------------------------------------------------------------------
  # 4. Spike detection -- DISABLED (matches stage_qc_pipeline.R)
  # ---------------------------------------------------------------------------
  # Was flagging real events as spikes in the stage pipeline -- needs a
  # persistence/recovery check before it can be enabled anywhere. Kept off.


  # ---------------------------------------------------------------------------
  # 5. Gap fill -- Tier 1: reference-series linear relationship (LARGE
  #    transmission gaps only). Skips cleanly if `reference` is NULL/empty.
  # ---------------------------------------------------------------------------
  df <- df |>
    mutate(
      fill_method = NA_character_,
      is_transmission_gap = origin == "transmission_gap",
      gap_id = cumsum(is_transmission_gap & !lag(is_transmission_gap, default = FALSE))
    )

  ref_gap_info <- df |>
    filter(is_transmission_gap) |>
    group_by(gap_id) |>
    summarise(
      gap_mins = as.numeric(difftime(max(measurement_time), min(measurement_time), units = "mins")),
      .groups  = "drop"
    )
  large_gap_ids <- ref_gap_info |> filter(gap_mins >= p$min_gap_for_ref_mins) |> pull(gap_id)

  message(length(large_gap_ids), " transmission gaps qualify for reference-series filling (>= ",
          p$min_gap_for_ref_mins, " min); ",
          nrow(ref_gap_info) - length(large_gap_ids), " smaller gaps left for spline")

  df_ref <- df |> left_join(ref, by = "measurement_time")
  fit_data <- df_ref |> filter(origin == "raw", !is.na(value_qc), !is.na(ref_value))
  ref_relationship_usable <- nrow(fit_data) >= 100 && length(large_gap_ids) > 0
  fit <- NULL

  if (length(large_gap_ids) == 0) {
    message("No gaps meet the reference-series threshold -- reference gap-filling skipped")
  } else if (nrow(fit_data) < 100) {
    message("Insufficient clean overlapping reference data (", nrow(fit_data),
            " rows) -- reference gap-filling skipped, falling through to spline")
  } else {
    fit <- lm(value_qc ~ ref_value, data = fit_data)
    r2  <- summary(fit)$r.squared
    message("Reference relationship R2 = ", round(r2, 4))
    if (r2 < p$ref_r2_threshold) {
      message("R2 below threshold (", p$ref_r2_threshold, ") -- reference gap-filling skipped")
      ref_relationship_usable <- FALSE
    }
  }

  if (ref_relationship_usable) {
    df_ref <- df_ref |>
      mutate(
        ref_predicted = predict(fit, newdata = data.frame(ref_value = ref_value)),
        ref_eligible  = is_transmission_gap & (gap_id %in% large_gap_ids) & !is.na(ref_predicted),
        value_qc      = if_else(ref_eligible, ref_predicted, value_qc),
        fill_method   = if_else(ref_eligible, "ref", fill_method)
      )
    message("Rows filled via reference relationship (transmission gaps): ",
            sum(df_ref$fill_method == "ref", na.rm = TRUE))
  }

  df <- df_ref |>
    select(-any_of(c("ref_value", "ref_predicted", "ref_eligible",
                     "gap_id", "is_transmission_gap")))


  # ---------------------------------------------------------------------------
  # 5b. Reconstruct the primary series inside bad-data windows from the
  #     reference series
  # ---------------------------------------------------------------------------
  ref_recon_plot <- NULL

  if (any(df$origin == "bad_data") && nrow(ref) > 0) {

    bad_start <- min(bad_windows$start)
    fit_from  <- bad_start - lubridate::days(p$ref_fit_window_days)
    ref_max_gap_rows <- ceiling(p$ref_max_interp_mins / p$interval_min)

    df <- df |> left_join(ref, by = "measurement_time")
    df$ref_value_grid <- if (sum(!is.na(df$ref_value)) >= 2) {
      zoo::na.approx(df$ref_value, x = df$measurement_time, na.rm = FALSE,
                     maxgap = ref_max_gap_rows)
    } else {
      NA_real_
    }

    ref_fit_data <- df |>
      filter(origin == "raw", !is.na(value_qc), !is.na(ref_value_grid),
             measurement_time >= fit_from, measurement_time < bad_start)

    if (nrow(ref_fit_data) < 100) {
      warning("Only ", nrow(ref_fit_data), " concurrent reference/primary points in the ",
              p$ref_fit_window_days, "-day window before ", format(bad_start, "%Y-%m-%d"),
              " -- reference reconstruction NOT applied; bad_data rows stay MV.")
      df <- df |> select(-any_of(c("ref_value", "ref_value_grid")))
    } else {
      ref_fit  <- lm(value_qc ~ splines::ns(ref_value_grid, df = p$ref_fit_spline_df),
                    data = ref_fit_data)
      ref_r2   <- summary(ref_fit)$r.squared
      ref_rmse <- sqrt(mean(residuals(ref_fit)^2))
      message("Reference -> primary reconstruction fit: ", nrow(ref_fit_data), " points, ",
              format(fit_from, "%Y-%m-%d"), " to ", format(bad_start, "%Y-%m-%d"),
              " | ns df = ", p$ref_fit_spline_df,
              " | R2 = ", round(ref_r2, 4), " | RMSE = ", round(ref_rmse, 4))
      if (ref_r2 < 0.9) {
        warning("Reference -> primary fit R2 is only ", round(ref_r2, 3),
                " -- inspect attr(result, 'ref_recon_plot') before trusting it.")
      }

      df$ref_recon <- predict(ref_fit, newdata = df, na.action = na.pass)
      df <- df |>
        mutate(
          recon_ok    = origin == "bad_data" & !is.na(ref_recon),
          value_qc    = if_else(recon_ok, ref_recon, value_qc),
          fill_method = if_else(recon_ok, "ref_recon", fill_method)
        )
      message("  reconstructed ", sum(df$fill_method == "ref_recon", na.rm = TRUE),
              " rows from the reference series; ",
              sum(df$origin == "bad_data" & is.na(df$value_qc)),
              " bad rows have no reference coverage (stay MV)")

      ref_grid_seq <- seq(min(ref_fit_data$ref_value_grid), max(ref_fit_data$ref_value_grid),
                         length.out = 200)
      ref_recon_plot <- tibble(
          ref_value_grid = ref_grid_seq,
          primary_pred   = predict(ref_fit, newdata = tibble(ref_value_grid = ref_grid_seq))
        ) |>
        ggplot(aes(ref_value_grid, primary_pred)) +
        geom_point(data = ref_fit_data, aes(y = value_qc), alpha = 0.15, size = 0.5) +
        geom_line(colour = "firebrick", linewidth = 0.9) +
        labs(x = "Reference series value", y = "Primary value",
             title = glue::glue("Reference -> primary relationship (ns df {p$ref_fit_spline_df}) -- R2 {round(ref_r2, 3)}"),
             subtitle = glue::glue("Fit window {format(fit_from, '%Y-%m-%d')} to {format(bad_start, '%Y-%m-%d')}"))

      df <- df |> select(-any_of(c("ref_value", "ref_value_grid", "ref_recon", "recon_ok")))
    }
  }


  # ---------------------------------------------------------------------------
  # 6. Gap fill -- Tier 2: spline interpolation
  # ---------------------------------------------------------------------------
  force_fill_windows <- .qc_prep_windows(p$force_fill_windows, df$measurement_time)

  df <- df |>
    mutate(
      # Exclude bad-data rows AND anything inside a bad-data window -- see
      # the note in section 3b.
      is_gap = is.na(value_qc) & origin != "bad_data" & !in_bad_window,
      gap_id = cumsum(is_gap & !lag(is_gap, default = FALSE))
    )

  gap_info <- df |>
    filter(is_gap) |>
    group_by(gap_id) |>
    summarise(
      start    = min(measurement_time),
      end      = max(measurement_time),
      gap_mins = as.numeric(difftime(max(measurement_time), min(measurement_time), units = "mins")),
      .groups  = "drop"
    ) |>
    rowwise() |>
    mutate(
      force_fill = nrow(force_fill_windows) > 0 &&
        any(start <= force_fill_windows$end & end >= force_fill_windows$start)
    ) |>
    ungroup()

  fillable_ids <- gap_info |>
    filter(gap_mins <= p$max_fill_gap_mins | force_fill) |> pull(gap_id)

  if (any(gap_info$force_fill)) {
    message(sum(gap_info$force_fill), " gap(s) forced fillable via force_fill_windows")
  }
  message(length(fillable_ids), " remaining gaps eligible for spline, ",
          nrow(gap_info) - length(fillable_ids), " too long -- left as NA")

  df <- df |>
    left_join(gap_info, by = "gap_id") |>
    mutate(
      value_for_spline = if_else(is_gap & !(gap_id %in% fillable_ids), NA_real_, value_qc),
      value_splined    = zoo::na.spline(value_for_spline, na.rm = FALSE),
      fill_method = if_else(is_gap & gap_id %in% fillable_ids, "spline", fill_method),
      value_qc    = if_else(is_gap & gap_id %in% fillable_ids, value_splined, value_qc)
    )

  # na.spline() can return NA at record edges, or an absurd finite value when
  # a fillable gap is bracketed by too little/too distant real data (cubic
  # extrapolation overshoots). Reject both -- downgrade to unfilled anything
  # "filled" with NA or a value outside the plausible range (if one is set),
  # rather than shipping a false "filled" flag.
  spline_bad <- df$fill_method == "spline" & is.na(df$value_qc)
  if (!is.na(p$range_min) && !is.na(p$range_max)) {
    spline_bad <- spline_bad | (df$fill_method == "spline" &
      (df$value_qc < p$range_min | df$value_qc > p$range_max))
  }
  spline_bad[is.na(spline_bad)] <- FALSE
  n_spline_bad <- sum(spline_bad)

  df <- df |>
    mutate(
      value_qc    = if_else(spline_bad, NA_real_, value_qc),
      fill_method = if_else(spline_bad, NA_character_, fill_method)
    ) |>
    select(-is_gap, -gap_id, -gap_mins, -start, -end, -force_fill,
           -value_for_spline, -value_splined)
  if (n_spline_bad > 0) {
    message("WARNING -- ", n_spline_bad,
            " rows flagged spline but na.spline() returned NA or an out-of-range ",
            "value. Downgraded to unfilled.")
  }

  # Same plausibility guard for the reference-relationship fills (ref) and
  # the reference reconstruction (ref_recon), when a range is set.
  if (!is.na(p$range_min) && !is.na(p$range_max)) {
    ref_bad <- df$fill_method %in% c("ref", "ref_recon") &
      (is.na(df$value_qc) | df$value_qc < p$range_min | df$value_qc > p$range_max)
    ref_bad[is.na(ref_bad)] <- FALSE
    if (sum(ref_bad) > 0) {
      message("WARNING -- ", sum(ref_bad),
              " reference-filled rows predicted an out-of-range value. Downgraded to unfilled.")
      df <- df |>
        mutate(
          value_qc    = if_else(ref_bad, NA_real_, value_qc),
          fill_method = if_else(ref_bad, NA_character_, fill_method)
        )
    }
  }


  # ---------------------------------------------------------------------------
  # 7. Combine origin + fill_method into one qc_flag label
  # ---------------------------------------------------------------------------
  df <- df |>
    mutate(
      qc_flag = case_when(
        origin == "raw" & !is.na(offset_applied)                ~ "offset_corrected",
        origin == "raw"                                         ~ "raw",
        origin == "flatline"                                    ~ "flagged_flatline",
        fill_method == "ref"                                    ~ "gf_ref",
        fill_method == "ref_recon"                               ~ "recon_ref",
        fill_method == "spline" & origin == "transmission_gap"  ~ "gf_spline",
        fill_method == "spline" & origin == "range_fail"        ~ "replaced_range",
        origin == "bad_data"                                    ~ "bad_data",
        TRUE                                                     ~ paste0("unfilled_", origin)
      ),
      # Wherever there's no value to report, there's no stat to report either
      # -- same rule qc_format_upload()/qc_validate_upload() enforce downstream.
      value_min = if_else(is.na(value_qc), NA_real_, value_min),
      value_max = if_else(is.na(value_qc), NA_real_, value_max),
      value_std = if_else(is.na(value_qc), NA_real_, value_std)
    ) |>
    select(measurement_time, value, value_qc, origin, fill_method,
           offset_applied, qc_flag, value_min, value_max, value_std)

  message("\n--- QC flag summary ---")
  df |> count(qc_flag) |> print(n = Inf)

  attr(df, "ref_recon_plot") <- ref_recon_plot
  df
}
