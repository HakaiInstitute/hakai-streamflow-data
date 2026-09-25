# =============================================================================
# qc_generic_upload.R -- generic DB-upload formatting + guardrails
# =============================================================================
# Source stage_qc_functions.R (for sn_read_qc / sn_qc_table_name, reused
# as-is) and qc_generic_pipeline.R (for qc_default_params / qc_run()) first,
# then this.
#
# Targets the `sn/qc/:tableName` upload path. Generalised so
# `measurement_name` can be anything, not just a Lvl channel -- so is NOT
# upload_ready.R's Telemetry-Network CSV format (WtrLvl<gen><site>-prefixed
# columns, 4-row header, confirmed to use a per-site column-naming scheme
# that isn't a fixed formula -- see the Hecate vs WSN693_703 examples in
# conversation). That format is deliberately NOT generalised here.
#
# CONFIRMED SCHEMA -- based on a real script that actually executes
# hakai_client$patch() against the live API (unlike pls-workflow.R section
# 11's loop, which was commented out and never run). The exercised payload
# has exactly 7 columns: measurement_time, quality_level, qc_flag, **val**
# (not `avg` -- pls-workflow.R's untested draft used `avg`; the real,
# executed script uses `val`), measurement_name, qc_by, recorded_time.
# No unesco_q_level/min/max/std in that confirmed payload -- those belong to
# the Telemetry CSV format instead. qc_format_upload() defaults to this
# exact 7-column shape; the extra stat fields are available via
# `include_stats = TRUE` but are NOT confirmed against the live API, so
# they're opt-in with a loud caveat rather than the default.
#
# The confirmed script also uses PATCH, not POST. Hakai's API may accept
# either depending on the intended semantics (POST to add new records, PATCH
# to modify existing ones) -- sn_post_qc() takes `method` so you choose.
#
# value_min/value_max/value_std, if you want them for `include_stats = TRUE`,
# are carried natively through qc_run() (qc_generic_pipeline.R) -- pass
# sn_read_measurement()'s output straight in as `primary` and they ride along
# through the same offset/MV decisions `value` gets. qc_format_upload() below
# reads value_min/value_max/value_std directly off whatever qc_run() returned.
#
# Sections:
#   A. qc_format_upload()   -- qc tibble -> the confirmed sn/qc upload shape
#   B. qc_validate_upload() -- the guardrail: 0-row tibble = clean to upload
#   C. sn_post_qc()         -- PATCHes (or POSTs) but only after
#                              qc_validate_upload() passes (hard stop
#                              otherwise, never automatic)
# =============================================================================

library(tidyverse)
library(glue)


# #############################################################################
# A. qc_format_upload() -- build the sn/qc upload tibble
# #############################################################################
#' Format a [qc_run()] result for upload to `api/sn/qc/:tableName`
#'
#' Generalises `to_db_upload()` (`run_stage_qc_ssn703.R`) / pls-workflow.R
#' sections 8-9: derives `quality_level` (2 = raw/unfilled, 3 = filled or
#' offset-corrected) and an AV/EV/MV `qc_flag` description string from the
#' `qc_flag` column [qc_run()] produces. Defaults to the exact 7-column
#' shape confirmed against the live API by a real, executed upload script
#' (see file header) -- including the column name `val`, not `avg`.
#'
#' @param qc A tibble from [qc_run()]. For `include_stats = TRUE`, pass
#'   [sn_read_measurement()]'s output straight into `qc_run(primary = ...)`
#'   beforehand -- `value_min`/`value_max`/`value_std` are then already on
#'   `qc` and read straight off it here, no separate join step needed.
#' @param measurement_name The Hakai `measurement_name` this series uploads
#'   under (e.g. `"RH"`, `"AirTemp"`, `"PLS_Lvl"`).
#' @param qc_by Who ran the QC (email), recorded as-is on every row.
#' @param params The same `params` list passed to [qc_run()] -- used to
#'   embed the actual thresholds into the flag description text.
#' @param flag_text Optional named character vector, `qc_flag value = text`,
#'   to override the default description for specific flags (e.g. different
#'   wording for a particular variable). Any `qc_flag` value not named here
#'   falls back to the built-in default mapping.
#' @param include_stats Default `FALSE` -- adds `unesco_q_level`, `min`,
#'   `max`, `std` on top of the confirmed 7 columns, using `value_min`/
#'   `value_max`/`value_std` straight off `qc` if present (`NA` otherwise --
#'   see the `qc` param above for how to get real stats onto `qc` in the
#'   first place). **Not confirmed against the live API** -- only the
#'   7-column default shape has an example of an actual successful upload
#'   behind it. Turn this on only if you've separately confirmed the
#'   endpoint accepts these extra fields.
#' @return A tibble: `measurement_time`, `quality_level`, `qc_flag`,
#'   `measurement_name`, `qc_by`, `recorded_time`, `val` -- plus
#'   `unesco_q_level`/`min`/`max`/`std` if `include_stats = TRUE`.
#' @export
qc_format_upload <- function(qc, measurement_name, qc_by, params = qc_default_params(),
                              flag_text = NULL, include_stats = FALSE) {
  p <- params

  default_text <- function(qc_flag) {
    case_when(
      qc_flag == "offset_corrected" ~ "AV:EV: Offset applied to align sensor to the reference datum",
      qc_flag == "gf_ref"           ~ "AV:EV: Transmission gap filled using reference-sensor relationship",
      qc_flag == "recon_ref"        ~ glue("AV:EV: Primary sensor failed; series reconstructed from the reference sensor via fitted spline relationship ({p$ref_fit_window_days}-day fit window)"),
      qc_flag == "gf_spline"        ~ "AV:EV: Transmission gap filled via spline interpolation",
      qc_flag == "replaced_range"   ~ glue("AV:EV: Value outside plausible range [{p$range_min}, {p$range_max}], corrected via spline interpolation"),
      qc_flag == "bad_data"         ~ "MV: Primary sensor failed and no reference coverage to reconstruct from",
      qc_flag == "flagged_flatline" ~ glue("PV: Flatlined for >= {p$flatline_window_days} days -- value NOT modified, flagged for review only"),
      str_starts(qc_flag, "unfilled") ~ glue("MV: No value available ({str_remove(qc_flag, 'unfilled_')}, gap > {p$max_fill_gap_mins} min or no bracketing data)"),
      TRUE ~ "AV"
    )
  }

  qc_flag_code <- default_text(qc$qc_flag)

  if (!is.null(flag_text)) {
    override <- unname(flag_text[qc$qc_flag])
    qc_flag_code <- if_else(!is.na(override), override, qc_flag_code)
  }

  out <- qc |>
    mutate(
      quality_level  = if_else(!is.na(fill_method) | qc_flag == "offset_corrected", 3, 2),
      qc_flag_code   = qc_flag_code
    ) |>
    transmute(
      measurement_time = strftime(measurement_time, "%Y-%m-%dT%H:%M:%S%z"),
      quality_level    = quality_level,
      qc_flag          = qc_flag_code,
      measurement_name = measurement_name,
      qc_by            = qc_by,
      recorded_time    = strftime(lubridate::now(), "%Y-%m-%dT%H:%M:%S%z"),
      val              = value_qc
    )

  if (include_stats) {
    has_stats <- all(c("value_min", "value_max", "value_std") %in% names(qc)) &&
      !all(is.na(qc$value_min) & is.na(qc$value_max) & is.na(qc$value_std))
    if (!has_stats) {
      message("qc_format_upload(include_stats = TRUE): no real value_min/value_max/value_std ",
              "found on `qc` -- min/max/std will be NA throughout. Pass sn_read_measurement()'s ",
              "output straight into qc_run() as `primary` to carry real stats through.")
    }
    message("qc_format_upload(include_stats = TRUE): adding unesco_q_level/min/max/std -- ",
            "these are NOT confirmed against the live sn/qc endpoint, only the default ",
            "7-column shape is. Confirm separately before uploading.")

    out <- out |>
      mutate(
        unesco_q_level = if_else(is.na(val), 9, 1),
        min = if ("value_min" %in% names(qc)) qc$value_min else NA_real_,
        max = if ("value_max" %in% names(qc)) qc$value_max else NA_real_,
        std = if ("value_std" %in% names(qc)) qc$value_std else NA_real_
      )
  }

  n_missing <- sum(is.na(out$quality_level) | is.na(out$qc_flag))
  if (n_missing > 0) {
    warning(n_missing, " row(s) have no resolved quality_level/qc_flag -- ",
            "a qc_flag value from qc_run() isn't covered by default_text()/flag_text. ",
            "qc_validate_upload() will also catch this before any upload.")
  }

  out
}


# #############################################################################
# B. qc_validate_upload() -- the guardrail
# #############################################################################
#' Validate an upload tibble before it goes anywhere near `sn_post_qc()`
#'
#' Generalises the ad hoc `missing_quality` check in pls-workflow.R section 8
#' into an always-run gate covering completeness, internal consistency, exact
#' date/time format, duplicate timestamps, stray/invalid characters in text
#' fields, and row-count/time-range parity against the source `qc` tibble --
#' plus an optional live cross-check against the table's actual QC history.
#'
#' @param upload A tibble from [qc_format_upload()].
#' @param qc The [qc_run()] tibble `upload` was built from (for parity
#'   checks).
#' @param client,table_name Optional: if both given, cross-checks
#'   `measurement_name` against what's actually on record in that QC table
#'   via [sn_read_qc()] (reused, unmodified) -- a `measurement_name` that's
#'   never appeared there before is a strong signal of a typo/wrong mapping.
#' @return A tibble of problems: `check`, `measurement_time` (NA for
#'   dataset-level problems), `detail`. **0 rows means clean.**
#' @export
qc_validate_upload <- function(upload, qc, client = NULL, table_name = NULL) {

  flag_problem <- function(check, rows, detail) {
    if (length(rows) == 0) return(NULL)
    tibble(check = check, measurement_time = upload$measurement_time[rows], detail = detail)
  }

  problems <- list()

  # -- 1. Completeness -- (unesco_q_level only required if include_stats was used)
  required_complete <- c("quality_level", "qc_flag", "measurement_name", "qc_by", "recorded_time")
  if ("unesco_q_level" %in% names(upload)) required_complete <- c(required_complete, "unesco_q_level")
  na_rows <- which(Reduce(`|`, lapply(upload[required_complete], is.na)))
  problems$completeness <- flag_problem(
    "completeness", na_rows,
    glue("NA in a required field ({paste(required_complete, collapse = '/')})")
  )

  # -- 2. Value/flag consistency --
  mv_flag <- str_detect(upload$qc_flag, "MV")
  mismatch_rows <- which(xor(is.na(upload$val), mv_flag))
  problems$value_flag <- flag_problem(
    "value_flag_consistency", mismatch_rows,
    "val is NA precisely when qc_flag contains 'MV' -- these should always agree"
  )

  bad_ql <- which(!upload$quality_level %in% c(2, 3))
  problems$quality_level <- flag_problem(
    "quality_level_range", bad_ql, "quality_level is not 2 or 3"
  )

  # -- unesco_q_level / min/max/std checks only apply when include_stats was used --
  if ("unesco_q_level" %in% names(upload)) {
    bad_uql <- which(!upload$unesco_q_level %in% c(1, 9))
    problems$unesco_q_level <- flag_problem(
      "unesco_q_level_range", bad_uql, "unesco_q_level is not 1 or 9"
    )

    uql_mismatch <- which(xor(is.na(upload$val), upload$unesco_q_level == 9))
    problems$uql_val_consistency <- flag_problem(
      "uql_val_consistency", uql_mismatch,
      "unesco_q_level is 9 precisely when val is NA -- these should always agree"
    )
  }

  if (all(c("min", "max", "std") %in% names(upload))) {
    # min/max/std should be NA exactly where val is NA (nothing to report) --
    # when val IS present they may legitimately still be NA if the sensor
    # never exposed that stat (see sn_read_measurement()), so only the MV
    # direction is checked here, not the reverse.
    stat_leftover <- which(is.na(upload$val) &
      (!is.na(upload$min) | !is.na(upload$max) | !is.na(upload$std)))
    problems$stat_mv_consistency <- flag_problem(
      "stat_mv_consistency", stat_leftover,
      "min/max/std should be NA wherever val is NA (no value to report), but at least one wasn't"
    )
  }

  # -- 3. Date/time format: regex AND parse-roundtrip --
  ts_pattern <- "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}[+-]\\d{4}$"
  bad_format <- which(!str_detect(upload$measurement_time, ts_pattern))
  problems$time_format_regex <- flag_problem(
    "time_format_regex", bad_format,
    "measurement_time does not match the expected \"%Y-%m-%dT%H:%M:%S%z\" shape"
  )

  parsed <- as.POSIXct(upload$measurement_time, format = "%Y-%m-%dT%H:%M:%S%z", tz = "UTC")
  reformatted <- strftime(parsed, "%Y-%m-%dT%H:%M:%S%z")
  roundtrip_bad <- which(is.na(parsed) | reformatted != upload$measurement_time)
  problems$time_roundtrip <- flag_problem(
    "time_format_roundtrip", roundtrip_bad,
    "measurement_time failed to parse as a valid instant, or reformatting it didn't reproduce the exact same string"
  )

  # -- 4. Duplicate timestamps --
  dup_rows <- which(duplicated(upload$measurement_time) | duplicated(upload$measurement_time, fromLast = TRUE))
  problems$duplicates <- flag_problem(
    "duplicate_timestamp", dup_rows, "duplicate measurement_time in the upload set"
  )

  # -- 5. Weird/invalid characters in text fields --
  text_cols <- c("qc_flag", "qc_by", "measurement_name")
  weird_rows <- integer(0)
  weird_detail <- character(0)
  for (col in text_cols) {
    vals <- upload[[col]]
    bad <- which(str_detect(vals, "[^\x20-\x7E]") | !validUTF8(vals))
    if (length(bad) > 0) {
      weird_rows   <- c(weird_rows, bad)
      weird_detail <- c(weird_detail, rep(glue("non-printable-ASCII or invalid-UTF8 character in '{col}'"), length(bad)))
    }
  }
  if (length(weird_rows) > 0) {
    problems$weird_chars <- tibble(
      check = "weird_characters",
      measurement_time = upload$measurement_time[weird_rows],
      detail = weird_detail
    )
  }

  # -- 6. Row-count / time-range parity against the source qc tibble --
  if (nrow(upload) != nrow(qc)) {
    problems$row_count_parity <- tibble(
      check = "row_count_parity", measurement_time = as.POSIXct(NA),
      detail = glue("upload has {nrow(upload)} rows but the source qc tibble has {nrow(qc)} rows")
    )
  } else if (all(!is.na(parsed))) {
    range_upload <- range(parsed, na.rm = TRUE)
    range_qc     <- range(qc$measurement_time, na.rm = TRUE)
    if (any(abs(as.numeric(difftime(range_upload, range_qc, units = "secs"))) > 1)) {
      problems$time_range_parity <- tibble(
        check = "time_range_parity", measurement_time = as.POSIXct(NA),
        detail = glue("upload time range [{range_upload[1]}, {range_upload[2]}] doesn't match ",
                      "the source qc tibble's range [{range_qc[1]}, {range_qc[2]}]")
      )
    }
  }

  # -- 7. Optional live cross-check against the table's real QC history --
  if (!is.null(client) && !is.null(table_name)) {
    measurement_name <- unique(upload$measurement_name)
    date_lo <- format(min(qc$measurement_time, na.rm = TRUE) - lubridate::days(1), "%Y-%m-%d")
    date_hi <- format(max(qc$measurement_time, na.rm = TRUE) + lubridate::days(1), "%Y-%m-%d")
    history <- sn_read_qc(client, table_name, date_lo, date_hi)

    if (nrow(history) > 0 && !all(measurement_name %in% history$measurement_name)) {
      problems$unknown_measurement_name <- tibble(
        check = "unknown_measurement_name", measurement_time = as.POSIXct(NA),
        detail = glue(
          "'{paste(setdiff(measurement_name, history$measurement_name), collapse = ', ')}' ",
          "has never appeared in {table_name}'s QC history -- confirm this is the right ",
          "measurement_name before uploading. Known names there: ",
          "{paste(unique(history$measurement_name), collapse = ', ')}"
        )
      )
    }
  }

  out <- bind_rows(problems)

  if (nrow(out) == 0) {
    message("qc_validate_upload(): clean -- 0 problems found across ", nrow(upload), " rows.")
  } else {
    message("qc_validate_upload(): ", nrow(out), " problem(s) found across ",
            length(unique(out$check)), " check(s) -- see returned tibble.")
  }

  out
}


# #############################################################################
# C. sn_post_qc() -- PATCH (or POST), but only once qc_validate_upload() passes
# #############################################################################
#' Upload a formatted, validated upload to `api/sn/qc/:tableName`
#'
#' Generalises the commented-out loop in pls-workflow.R section 11 -- except
#' that loop used `client$post()` and was never actually run. A separate,
#' real script that *does* execute against the live API uses
#' `client$patch()` instead, so that's the default here. Hakai's API may
#' accept either verb depending on intent (POST to add new rows, PATCH to
#' modify existing ones) -- pick with `method`.
#'
#' Always runs [qc_validate_upload()] first and refuses to upload anything
#' (a hard `stop()`, not a warning) if it returns any problem rows --
#' validate, fix, re-run, only then upload.
#'
#' @param client A client from [sn_connect()].
#' @param upload A tibble from [qc_format_upload()].
#' @param qc The [qc_run()] tibble `upload` was built from (passed through
#'   to [qc_validate_upload()]).
#' @param table_name From [sn_qc_table_name()].
#' @param method `"patch"` (default -- matches the one confirmed, actually
#'   executed real-world script) or `"post"`.
#' @param window_size Rows per batch. Default `1000`.
#' @export
sn_post_qc <- function(client, upload, qc, table_name, method = c("patch", "post"),
                        window_size = 1000) {
  method <- match.arg(method)
  problems <- qc_validate_upload(upload, qc, client = client, table_name = table_name)

  if (nrow(problems) > 0) {
    print(problems, n = Inf)
    stop(nrow(problems), " validation problem(s) found -- refusing to upload. ",
         "Fix the upload and re-run qc_validate_upload() before calling sn_post_qc() again.")
  }

  send <- if (method == "patch") client$patch else client$post

  message("Validation passed. ", toupper(method), "ing ", nrow(upload),
          " rows to api/sn/qc/", table_name, " in batches of ", window_size, "...")

  baseurl <- glue("api/sn/qc/{table_name}")
  n_batches <- ceiling(nrow(upload) / window_size)
  for (i in seq_len(n_batches) - 1) {
    lb <- i * window_size + 1
    ub <- min(nrow(upload), (i + 1) * window_size)
    send(baseurl, upload[lb:ub, ])
  }
  message("Done -- ", nrow(upload), " rows uploaded (", method, ") across ", n_batches, " batch(es).")
}
