# =============================================================================
# stage_qc_pipeline.R -- the stage QC engine (one sensor generation at a time)
# =============================================================================
# Source stage_qc_functions.R first, then this. Driven by run_stage_qc_<station>.R.
#
# Generalised from the old pls-workflow.R: that script hard-coded SSN703's PLS
# generation, its +0.02 m RC1 offset, and its bad-data window, and dumped
# everything into the global environment. Here it is one function --
# stage_qc_run() -- taking the raw series, an optional reference (SA) series,
# and a params list, and returning the QC'd tibble.
#
# What it does, in order (section numbers match the old pls-workflow.R):
#   1b. Datum offset       -- applied to raw value BEFORE any QC, from
#                             params$offset_windows (which comes from
#                             03_docs/metadata/offsets.csv, not hard-coded)
#   2.  Range check        -- value_qc nulled outside [range_min, range_max]
#   3.  Flatline check     -- flag-only (value_qc NOT nulled), rolling range
#   3b. Bad-data windows   -- manual; value_qc nulled, rebuilt from SA in 5b
#   4.  Spike detection    -- DISABLED (still over-captures real events)
#   5.  Gap fill tier 1    -- SA linear relationship, large transmission gaps only
#   5b. Bad-data recon     -- SA -> primary via fitted natural-spline relationship
#   6.  Gap fill tier 2    -- spline interpolation within max_fill_gap_mins
#   7.  origin + fill_method -> qc_flag
#
# All the correctness fixes from pls-workflow.R are kept: gap_id increments on
# gap STARTS not valid->valid transitions; spline-flagged-but-NA rows are
# downgraded to unfilled; window bounds are force_tz'd to the data's tz.
#
# Returns: a tibble with
#   measurement_time, value (offset-corrected raw), value_qc, origin,
#   fill_method, offset_applied_m, qc_flag
# with the SA->primary reconstruction diagnostic plot attached as
#   attr(result, "sa_recon_plot")   (NULL when no bad-data window was rebuilt)
# =============================================================================

library(zoo)       # rollmedian, na.spline, rollapply, na.approx
library(splines)   # ns()


# -----------------------------------------------------------------------------
# Default parameters -- override per sensor generation in the run script
# -----------------------------------------------------------------------------
stage_qc_default_params <- function() {
  list(
    # -- Range check --
    range_min = 0.02,    # unconfirmed as QC threshold vs sensor spec
    range_max = 2.8,

    # -- Flatline check (flag-only) --
    flatline_window_days = 3,
    flatline_tolerance_m = 0.005,   # placeholder -- tune from what it flags

    # -- Sampling interval (row <-> minutes conversions) --
    interval_min = 5,

    # -- Gap fill --
    max_fill_gap_mins   = 180,
    sa_r2_threshold     = 0.95,
    min_gap_for_sa_mins = 60,       # SA never used below this

    # -- Manual windows (tibbles; empty = no-op) --
    offset_windows = tibble(start = as.POSIXct(character()),
                            end   = as.POSIXct(character()),
                            offset_m = numeric()),
    force_fill_windows = tibble(start = as.POSIXct(character()),
                                end   = as.POSIXct(character())),
    bad_data_windows = tibble(start = as.POSIXct(character()),
                              end   = as.POSIXct(character())),

    # -- SA -> primary reconstruction (bad-data windows) --
    sa_fit_window_days = 365,   # trusted window before the bad period to fit on
    sa_fit_spline_df   = 4,     # ns() df on SA
    sa_max_interp_mins = 35     # max SA gap to interpolate onto the grid
  )
}


# -----------------------------------------------------------------------------
# Helper: normalise a window tibble's bounds, and fill an NA `end` with the
# end of the record.
#
# POSIXct comparison in R is instant-based (the tzone label is ignored), so a
# bound that is already a POSIXct is a correct instant and is left ALONE.
# Only a tz-naive character bound (e.g. "2017-10-12 23:59:59" typed inline in
# a config) is interpreted in the data's timezone. The earlier version
# force_tz()'d every bound to the data tz, which silently shifted UTC-parsed
# registry timestamps by the UTC->PST offset (8 h).
# -----------------------------------------------------------------------------
.stage_qc_prep_windows <- function(windows, measurement_time) {
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
# stage_qc_run() -- the engine
# -----------------------------------------------------------------------------
#' Run the stage QC pipeline for one sensor generation
#'
#' @param raw A tibble with `measurement_time` (POSIXct) and `value`
#'   (numeric) -- the raw primary series (e.g. one PLS generation).
#' @param sa Optional tibble with `measurement_time` and `value` for the
#'   reference/SA sensor. `NULL` or 0-row = no SA (tiers that need it skip
#'   cleanly).
#' @param params A list from [stage_qc_default_params()], with any element
#'   overridden. `offset_windows` should carry this sensor's datum offset.
#' @return A tibble (see file header) with `attr(., "sa_recon_plot")`.
#' @export
stage_qc_run <- function(raw, sa = NULL, params = stage_qc_default_params()) {

  # NB: don't use modifyList() here -- it mangles the tibble-valued params
  # (offset_windows / bad_data_windows / force_fill_windows) into broken
  # column-wise merges. Replace named elements wholesale instead.
  p <- stage_qc_default_params()
  p[names(params)] <- params
  flatline_window_rows <- p$flatline_window_days * 24 * 60 / p$interval_min

  pls <- raw |> arrange(measurement_time) |> select(measurement_time, value)
  if (anyDuplicated(pls$measurement_time)) {
    n_dup <- sum(duplicated(pls$measurement_time))
    warning(n_dup, " duplicate timestamps in the primary series -- keeping the first of each.")
    pls <- pls |> distinct(measurement_time, .keep_all = TRUE)
  }

  sa  <- if (is.null(sa) || nrow(sa) == 0) {
    tibble(measurement_time = as.POSIXct(character()), stage_sa = numeric())
  } else {
    sa |> arrange(measurement_time) |>
      distinct(measurement_time, .keep_all = TRUE) |>   # a dup here would fan out pls on the join
      transmute(measurement_time, stage_sa = value)
  }

  message("stage_qc_run: ", nrow(pls), " primary rows, ", nrow(sa), " SA rows")


  # ---------------------------------------------------------------------------
  # 1b. Datum offset (from params$offset_windows -> offsets.csv)
  # ---------------------------------------------------------------------------
  pls$offset_applied_m <- NA_real_
  offset_windows <- .stage_qc_prep_windows(p$offset_windows, pls$measurement_time)

  for (i in seq_len(nrow(offset_windows))) {
    in_window <- pls$measurement_time >= offset_windows$start[i] &
      pls$measurement_time <= offset_windows$end[i]
    pls$value[in_window]            <- pls$value[in_window] + offset_windows$offset_m[i]
    pls$offset_applied_m[in_window] <- offset_windows$offset_m[i]
  }

  message("Datum offset: ", sum(!is.na(pls$offset_applied_m)), " rows adjusted across ",
          nrow(offset_windows), " window(s)")


  # ---------------------------------------------------------------------------
  # 2. Range check
  # ---------------------------------------------------------------------------
  pls <- pls |>
    mutate(
      origin   = if_else(is.na(value), "transmission_gap", "raw"),
      value_qc = value,
      origin   = if_else(origin == "raw" & (value_qc < p$range_min | value_qc > p$range_max),
                          "range_fail", origin),
      value_qc = if_else(origin == "range_fail", NA_real_, value_qc)
    )
  message("Range check: ", sum(pls$origin == "range_fail"),
          " rows outside [", p$range_min, ", ", p$range_max, "] m")


  # ---------------------------------------------------------------------------
  # 3. Flatline check (flag-only -- value_qc NOT nulled)
  # ---------------------------------------------------------------------------
  pls <- pls |>
    mutate(
      roll_range = zoo::rollapply(
        value_qc, width = flatline_window_rows,
        FUN = function(x) if (all(is.na(x))) NA_real_ else diff(range(x, na.rm = TRUE)),
        align = "center", fill = NA, partial = TRUE
      ),
      origin = if_else(origin == "raw" & !is.na(roll_range) & roll_range <= p$flatline_tolerance_m,
                        "flatline", origin)
    ) |>
    select(-roll_range)
  message("Flatline check: ", sum(pls$origin == "flatline"),
          " rows flatter than ", p$flatline_tolerance_m, " m over ",
          p$flatline_window_days, " days (flag-only, value kept)")


  # ---------------------------------------------------------------------------
  # 3b. Confirmed bad-data windows (manual)
  # ---------------------------------------------------------------------------
  bad_windows <- .stage_qc_prep_windows(p$bad_data_windows, pls$measurement_time)

  pls$in_bad_window <- FALSE
  if (nrow(bad_windows) > 0) {
    pls$in_bad_window <- purrr::reduce(
      purrr::map2(bad_windows$start, bad_windows$end,
                 ~ pls$measurement_time >= .x & pls$measurement_time <= .y),
      `|`
    )
    pls <- pls |>
      mutate(
        # origin label keeps the transmission_gap vs bad_data distinction, but
        # section 6 (spline) excludes ANY row inside a bad-data window -- see
        # the `in_bad_window` guard there. Interpolating a stray no-transmission
        # row that sits in the middle of a months-long bad-data block sends
        # na.spline() to +/-1e10.
        origin   = if_else(in_bad_window & origin != "transmission_gap", "bad_data", origin),
        value_qc = if_else(origin == "bad_data", NA_real_, value_qc)
      )
  }
  message("Bad-data windows: ", sum(pls$origin == "bad_data"),
          " rows flagged bad_data (rebuilt from SA in 5b where coverage allows)")


  # ---------------------------------------------------------------------------
  # 4. Spike detection -- DISABLED
  # ---------------------------------------------------------------------------
  # Was flagging real hydrological rises as spikes -- needs a persistence /
  # recovery check before it can be re-enabled. Kept off deliberately.


  # ---------------------------------------------------------------------------
  # 5. Gap fill -- Tier 1: SA linear relationship (LARGE transmission gaps)
  # ---------------------------------------------------------------------------
  pls <- pls |>
    mutate(
      fill_method = NA_character_,
      is_transmission_gap = origin == "transmission_gap",
      gap_id = cumsum(is_transmission_gap & !lag(is_transmission_gap, default = FALSE))
    )

  sa_gap_info <- pls |>
    filter(is_transmission_gap) |>
    group_by(gap_id) |>
    summarise(
      gap_mins = as.numeric(difftime(max(measurement_time), min(measurement_time), units = "mins")),
      .groups  = "drop"
    )
  large_gap_ids <- sa_gap_info |> filter(gap_mins >= p$min_gap_for_sa_mins) |> pull(gap_id)

  message(length(large_gap_ids), " transmission gaps qualify for SA filling (>= ",
          p$min_gap_for_sa_mins, " min); ",
          nrow(sa_gap_info) - length(large_gap_ids), " smaller gaps left for spline")

  pls_sa <- pls |> left_join(sa, by = "measurement_time")
  fit_data <- pls_sa |> filter(origin == "raw", !is.na(value_qc), !is.na(stage_sa))
  sa_relationship_usable <- nrow(fit_data) >= 100 && length(large_gap_ids) > 0
  fit <- NULL

  if (length(large_gap_ids) == 0) {
    message("No gaps meet the SA threshold -- SA gap-filling skipped")
  } else if (nrow(fit_data) < 100) {
    message("Insufficient clean overlapping SA data (", nrow(fit_data),
            " rows) -- SA gap-filling skipped, falling through to spline")
  } else {
    fit <- lm(value_qc ~ stage_sa, data = fit_data)
    r2  <- summary(fit)$r.squared
    message("SA relationship R2 = ", round(r2, 4))
    if (r2 < p$sa_r2_threshold) {
      message("R2 below threshold (", p$sa_r2_threshold, ") -- SA gap-filling skipped")
      sa_relationship_usable <- FALSE
    }
  }

  if (sa_relationship_usable) {
    pls_sa <- pls_sa |>
      mutate(
        stage_sa_predicted = predict(fit, newdata = data.frame(stage_sa = stage_sa)),
        sa_eligible = is_transmission_gap & (gap_id %in% large_gap_ids) & !is.na(stage_sa_predicted),
        value_qc    = if_else(sa_eligible, stage_sa_predicted, value_qc),
        fill_method = if_else(sa_eligible, "sa", fill_method)
      )
    message("Rows filled via SA relationship (transmission gaps): ",
            sum(pls_sa$fill_method == "sa", na.rm = TRUE))
  }

  pls <- pls_sa |>
    select(-any_of(c("stage_sa", "stage_sa_predicted", "sa_eligible",
                     "gap_id", "is_transmission_gap")))


  # ---------------------------------------------------------------------------
  # 5b. Reconstruct the primary series inside bad-data windows from SA
  # ---------------------------------------------------------------------------
  sa_recon_plot <- NULL

  if (any(pls$origin == "bad_data") && nrow(sa) > 0) {

    bad_start <- min(bad_windows$start)
    fit_from  <- bad_start - lubridate::days(p$sa_fit_window_days)
    sa_max_gap_rows <- ceiling(p$sa_max_interp_mins / p$interval_min)

    pls <- pls |> left_join(sa, by = "measurement_time")
    pls$stage_sa_grid <- if (sum(!is.na(pls$stage_sa)) >= 2) {
      zoo::na.approx(pls$stage_sa, x = pls$measurement_time, na.rm = FALSE,
                     maxgap = sa_max_gap_rows)
    } else {
      NA_real_
    }

    sa_fit_data <- pls |>
      filter(origin == "raw", !is.na(value_qc), !is.na(stage_sa_grid),
             measurement_time >= fit_from, measurement_time < bad_start)

    if (nrow(sa_fit_data) < 100) {
      warning("Only ", nrow(sa_fit_data), " concurrent SA/primary points in the ",
              p$sa_fit_window_days, "-day window before ", format(bad_start, "%Y-%m-%d"),
              " -- SA reconstruction NOT applied; bad_data rows stay MV.")
      pls <- pls |> select(-any_of(c("stage_sa", "stage_sa_grid")))
    } else {
      sa_fit  <- lm(value_qc ~ splines::ns(stage_sa_grid, df = p$sa_fit_spline_df),
                    data = sa_fit_data)
      sa_r2   <- summary(sa_fit)$r.squared
      sa_rmse <- sqrt(mean(residuals(sa_fit)^2))
      message("SA -> primary reconstruction fit: ", nrow(sa_fit_data), " points, ",
              format(fit_from, "%Y-%m-%d"), " to ", format(bad_start, "%Y-%m-%d"),
              " | ns df = ", p$sa_fit_spline_df,
              " | R2 = ", round(sa_r2, 4), " | RMSE = ", round(sa_rmse, 4), " m")
      if (sa_r2 < 0.9) {
        warning("SA -> primary fit R2 is only ", round(sa_r2, 3),
                " -- inspect attr(result, 'sa_recon_plot') before trusting it.")
      }

      pls$sa_recon <- predict(sa_fit, newdata = pls, na.action = na.pass)
      pls <- pls |>
        mutate(
          recon_ok    = origin == "bad_data" & !is.na(sa_recon),
          value_qc    = if_else(recon_ok, sa_recon, value_qc),
          fill_method = if_else(recon_ok, "sa_recon", fill_method)
        )
      message("  reconstructed ", sum(pls$fill_method == "sa_recon", na.rm = TRUE),
              " rows from SA; ",
              sum(pls$origin == "bad_data" & is.na(pls$value_qc)),
              " bad rows have no SA coverage (stay MV)")

      sa_grid_seq <- seq(min(sa_fit_data$stage_sa_grid), max(sa_fit_data$stage_sa_grid),
                         length.out = 200)
      sa_recon_plot <- tibble(
          stage_sa_grid = sa_grid_seq,
          primary_pred  = predict(sa_fit, newdata = tibble(stage_sa_grid = sa_grid_seq))
        ) |>
        ggplot(aes(stage_sa_grid, primary_pred)) +
        geom_point(data = sa_fit_data, aes(y = value_qc), alpha = 0.15, size = 0.5) +
        geom_line(colour = "firebrick", linewidth = 0.9) +
        labs(x = "SA sensor stage (m)", y = "Primary stage (m)",
             title = glue("SA -> primary relationship (ns df {p$sa_fit_spline_df}) -- R2 {round(sa_r2, 3)}"),
             subtitle = glue("Fit window {format(fit_from, '%Y-%m-%d')} to {format(bad_start, '%Y-%m-%d')}"))

      pls <- pls |> select(-any_of(c("stage_sa", "stage_sa_grid", "sa_recon", "recon_ok")))
    }
  }


  # ---------------------------------------------------------------------------
  # 6. Gap fill -- Tier 2: spline interpolation
  # ---------------------------------------------------------------------------
  force_fill_windows <- .stage_qc_prep_windows(p$force_fill_windows, pls$measurement_time)

  pls <- pls |>
    mutate(
      # Exclude bad-data rows AND anything inside a bad-data window (a stray
      # no-transmission row surrounded by months of bad_data must not be
      # spline-interpolated -- see the note in section 3b).
      is_gap = is.na(value_qc) & origin != "bad_data" & !in_bad_window,
      gap_id = cumsum(is_gap & !lag(is_gap, default = FALSE))
    )

  gap_info <- pls |>
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

  pls <- pls |>
    left_join(gap_info, by = "gap_id") |>
    mutate(
      value_for_spline = if_else(is_gap & !(gap_id %in% fillable_ids), NA_real_, value_qc),
      value_splined    = zoo::na.spline(value_for_spline, na.rm = FALSE),
      fill_method = if_else(is_gap & gap_id %in% fillable_ids, "spline", fill_method),
      value_qc    = if_else(is_gap & gap_id %in% fillable_ids, value_splined, value_qc)
    )

  # na.spline() can return NA at record edges, or an absurd finite value when
  # a fillable gap is bracketed by too little / too distant real data (cubic
  # extrapolation overshoots to +/-1e10). Reject both: downgrade to unfilled
  # anything the spline "filled" with NA or with a value outside the plausible
  # stage range -- don't ship a false "filled" flag.
  spline_bad <- pls$fill_method == "spline" &
    (is.na(pls$value_qc) | pls$value_qc < p$range_min | pls$value_qc > p$range_max)
  spline_bad[is.na(spline_bad)] <- FALSE
  n_spline_bad <- sum(spline_bad)

  pls <- pls |>
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

  # Same plausibility guard for the SA-relationship fills (gf_sa) and the SA
  # reconstruction (sa_recon): a linear fit or an ns() extrapolated well past
  # its fitted range can predict an out-of-range stage. Downgrade those too.
  sa_bad <- pls$fill_method %in% c("sa", "sa_recon") &
    (is.na(pls$value_qc) | pls$value_qc < p$range_min | pls$value_qc > p$range_max)
  sa_bad[is.na(sa_bad)] <- FALSE
  if (sum(sa_bad) > 0) {
    message("WARNING -- ", sum(sa_bad),
            " SA-filled rows predicted an out-of-range stage. Downgraded to unfilled.")
    pls <- pls |>
      mutate(
        value_qc    = if_else(sa_bad, NA_real_, value_qc),
        fill_method = if_else(sa_bad, NA_character_, fill_method)
      )
  }


  # ---------------------------------------------------------------------------
  # 7. Combine origin + fill_method into one qc_flag label
  # ---------------------------------------------------------------------------
  pls <- pls |>
    mutate(
      qc_flag = case_when(
        origin == "raw" & !is.na(offset_applied_m)             ~ "offset_corrected",
        origin == "raw"                                        ~ "raw",
        origin == "flatline"                                   ~ "flagged_flatline",
        fill_method == "sa"                                    ~ "gf_sa",
        fill_method == "sa_recon"                              ~ "recon_sa",
        fill_method == "spline" & origin == "transmission_gap" ~ "gf_spline",
        fill_method == "spline" & origin == "range_fail"       ~ "replaced_range",
        origin == "bad_data"                                  ~ "bad_data",
        TRUE                                                  ~ paste0("unfilled_", origin)
      )
    ) |>
    select(measurement_time, value, value_qc, origin, fill_method,
           offset_applied_m, qc_flag)

  message("\n--- QC flag summary ---")
  pls |> count(qc_flag) |> print(n = Inf)

  attr(pls, "sa_recon_plot") <- sa_recon_plot
  pls
}
