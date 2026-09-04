# =============================================================================
# pls_qc_workflow.R -- PLS_Lvl QC + Upload Prep
# =============================================================================
# Source qc_functions.R first.
# Range/flatline checks, SA + spline gap fill, bad-data reconstruction via SA.
# Spike detection disabled -- see section 4.
#
# SA sensor = SA_WTS703_PT / SensorDepth_Avg (separate site code, not under
# SSN703US). No valid data before ~Sept 2018.
#
# pls columns: origin = why flagged (raw/transmission_gap/range_fail/
# flatline/bad_data), fill_method = how fixed (NA/sa/sa_recon/spline),
# qc_flag = both combined for upload (origin/fill_method stay separate too).
#
# Inputs: pls_raw, sa_raw -- tibbles with measurement_time, value.
# =============================================================================

source("qc_functions.R")
library(zoo)   # rollmedian, na.spline, rollapply


# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

# -- Range check --
# RANGE_MIN (0.02) is unconfirmed as an actual QC threshold vs. sensor spec.
RANGE_MAX <- 2.8
RANGE_MIN <- 0.02

# -- Flatline check --
# FLATLINE_TOLERANCE_M is a placeholder -- tune from what it actually flags.
FLATLINE_WINDOW_DAYS  <- 3
FLATLINE_TOLERANCE_M  <- 0.005
FLATLINE_WINDOW_ROWS <- FLATLINE_WINDOW_DAYS * 24 * 60 / 5  # assumes 5-min PLS interval

# QC parameters -- see qc_diagnostics.R for how these were picked.
SPIKE_RATE_PER_5MIN <- 0.03
SPIKE_WINDOW         <- 5
MAX_FILL_GAP_MINS    <- 180
SA_R2_THRESHOLD      <- 0.95
MIN_GAP_FOR_SA_MINS  <- 60  # SA never used below this, or for spike/range/flatline origin

# Manual exceptions -- gaps to spline-fill despite exceeding MAX_FILL_GAP_MINS.
FORCE_FILL_WINDOWS <- tibble(
  start = as.POSIXct(character()),
  end   = as.POSIXct(character())
)
# e.g.:
# FORCE_FILL_WINDOWS <- tibble(
#   start = as.POSIXct("2014-09-10 16:40:00"),
#   end   = as.POSIXct("2014-09-11 00:15:00")
# )

# -- Confirmed bad-data periods (manual) --
# Sensor reporting but values wrong end to end (not a transmission gap).
# Rebuilt from SA in section 5b; end = NA means to end of record.
BAD_DATA_WINDOWS <- tibble(
  start = as.POSIXct("2023-06-25 00:00:00"),
  end   = as.POSIXct(NA)
)

# SA -> PLS3 reconstruction (section 5b):
SA_FIT_WINDOW_DAYS <- 365   # trusted window before the bad period to fit on
SA_FIT_SPLINE_DF   <- 4     # ns() df on SA -- allows the relationship to curve
SA_MAX_INTERP_MINS <- 35    # max SA gap to interpolate; longer stays NA (-> MV)

MEASUREMENT_NAME <- "PLS_Lvl3"
SITE             <- "SSN703US"
QC_BY            <- "emily.haughton@hakai.org"


# -----------------------------------------------------------------------------
# 1. Load data
# -----------------------------------------------------------------------------
pls <- pls_raw |> arrange(measurement_time)
sa  <- sa_raw  |> arrange(measurement_time) |> select(measurement_time, stage_sa = value)

message("Loaded ", nrow(pls), " rows for ", MEASUREMENT_NAME)
message("Loaded ", nrow(sa), " rows of SA sensor data")


# -----------------------------------------------------------------------------
# 2. Range check
# -----------------------------------------------------------------------------
pls <- pls |>
  mutate(
    origin   = if_else(is.na(value), "transmission_gap", "raw"),
    value_qc = value,
    origin   = if_else(origin == "raw" & (value_qc < RANGE_MIN | value_qc > RANGE_MAX),
                        "range_fail", origin),
    value_qc = if_else(origin == "range_fail", NA_real_, value_qc)
  )

n_range_fail <- sum(pls$origin == "range_fail")
message("Range check: ", n_range_fail, " rows outside [", RANGE_MIN, ", ", RANGE_MAX, "] m")


# -----------------------------------------------------------------------------
# 3. Flatline check
# -----------------------------------------------------------------------------
# Centered rolling range over FLATLINE_WINDOW_ROWS, on value_qc (range_fail
# already removed). Not fast on a full multi-year record.
pls <- pls |>
  mutate(
    roll_range = zoo::rollapply(
      value_qc, width = FLATLINE_WINDOW_ROWS,
      FUN = function(x) if (all(is.na(x))) NA_real_ else diff(range(x, na.rm = TRUE)),
      align = "center", fill = NA, partial = TRUE
    ),
    origin = if_else(origin == "raw" & !is.na(roll_range) & roll_range <= FLATLINE_TOLERANCE_M,
                      "flatline", origin)
    # value_qc NOT nulled -- flatline is flag-only, so gap-filling (which
    # only acts on is.na(value_qc)) never touches it.
  ) |>
  select(-roll_range)

n_flatline <- sum(pls$origin == "flatline")
message("Flatline check: ", n_flatline, " rows in a stretch flatter than ",
        FLATLINE_TOLERANCE_M, " m over ", FLATLINE_WINDOW_DAYS, " days")


# -----------------------------------------------------------------------------
# 3b. Confirmed bad-data periods (manual)
# -----------------------------------------------------------------------------
# Rows in BAD_DATA_WINDOWS get origin = "bad_data", value_qc nulled.
# transmission_gap stays transmission_gap. Rebuilt from SA in 5b; spline
# (section 6) is kept off these rows.
tz_data <- attr(pls$measurement_time, "tzone")
if (is.null(tz_data) || !nzchar(tz_data)) tz_data <- ""

if (nrow(BAD_DATA_WINDOWS) > 0) {
  bad_windows <- BAD_DATA_WINDOWS |>
    mutate(
      start = lubridate::force_tz(as.POSIXct(start), tz_data),
      end   = lubridate::force_tz(as.POSIXct(end),   tz_data),
      end   = dplyr::coalesce(end, max(pls$measurement_time, na.rm = TRUE))
    )

  in_bad_window <- purrr::reduce(
    purrr::map2(bad_windows$start, bad_windows$end,
               ~ pls$measurement_time >= .x & pls$measurement_time <= .y),
    `|`
  )

  pls <- pls |>
    mutate(
      origin   = if_else(in_bad_window & origin != "transmission_gap", "bad_data", origin),
      value_qc = if_else(origin == "bad_data", NA_real_, value_qc)
    )
}

n_bad <- sum(pls$origin == "bad_data")
message("Bad-data periods: ", n_bad, " rows flagged bad_data and nulled ",
        "(rebuilt from SA in section 5b where coverage allows)")


# -----------------------------------------------------------------------------
# 4. Spike detection -- DISABLED, still falsely capturing real events
# -----------------------------------------------------------------------------
# Was flagging real hydrological rises as spikes -- needs a persistence/
# recovery check.
#
# pls <- pls |>
#   mutate(
#     value_diff  = abs(value_qc - lag(value_qc)),
#     rolling_med = rollmedian(value_qc, k = SPIKE_WINDOW, fill = NA, align = "center"),
#     rolling_dev = abs(value_qc - rolling_med),
#     is_spike    = origin == "raw" &
#       (value_diff > SPIKE_RATE_PER_5MIN | rolling_dev > 3 * SPIKE_RATE_PER_5MIN),
#     origin      = if_else(is_spike & !is.na(is_spike), "spike", origin),
#     value_qc    = if_else(origin == "spike", NA_real_, value_qc)
#   ) |>
#   select(-value_diff, -rolling_med, -rolling_dev, -is_spike)
#
# n_spikes <- sum(pls$origin == "spike")
# message("Spikes flagged: ", n_spikes, " rows")


# -----------------------------------------------------------------------------
# 5. Gap filling -- Tier 1: SA sensor relationship (LARGE transmission gaps only)
# -----------------------------------------------------------------------------
# SA only for transmission gaps >= MIN_GAP_FOR_SA_MINS. bad_data windows get
# their own reconstruction in section 5b.
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

large_gap_ids <- sa_gap_info |> filter(gap_mins >= MIN_GAP_FOR_SA_MINS) |> pull(gap_id)

message(nrow(sa_gap_info |> filter(gap_mins >= MIN_GAP_FOR_SA_MINS)),
        " transmission gaps qualify for SA filling (>= ", MIN_GAP_FOR_SA_MINS, " min), ",
        nrow(sa_gap_info |> filter(gap_mins < MIN_GAP_FOR_SA_MINS)),
        " smaller gaps left for spline")

pls_sa <- pls |> left_join(sa, by = "measurement_time")

fit_data <- pls_sa |>
  filter(origin == "raw", !is.na(value_qc), !is.na(stage_sa))

sa_relationship_usable <- nrow(fit_data) >= 100 && length(large_gap_ids) > 0

if (length(large_gap_ids) == 0) {
  message("No gaps meet the ", MIN_GAP_FOR_SA_MINS, "-minute threshold -- SA gap-filling skipped")
} else if (nrow(fit_data) < 100) {
  message("Insufficient clean overlapping SA data (", nrow(fit_data),
          " rows) -- SA gap-filling skipped, falling through to spline")
} else {
  fit <- lm(value_qc ~ stage_sa, data = fit_data)
  r2  <- summary(fit)$r.squared
  message("SA relationship R2 = ", round(r2, 4))

  if (r2 < SA_R2_THRESHOLD) {
    message("R2 below threshold (", SA_R2_THRESHOLD, ") -- SA gap-filling skipped")
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


# -----------------------------------------------------------------------------
# 5b. Reconstruct the primary series inside bad-data windows from SA
# -----------------------------------------------------------------------------
# 1. Interpolate SA onto the 5-min grid (gaps > SA_MAX_INTERP_MINS stay NA)
# 2. Fit SA -> PLS3 on SA_FIT_WINDOW_DAYS of raw data before the bad period
#    (natural spline on SA -- straight-line fit diverged at peaks)
# 3. Predict across the bad window; no SA coverage stays MV
# Diagnostic plot left in sa_recon_plot.

bad_data_present <- any(pls$origin == "bad_data")

if (bad_data_present) {

  bad_start <- BAD_DATA_WINDOWS |>
    transmute(start = lubridate::force_tz(as.POSIXct(start), tz_data)) |>
    pull(start) |>
    min()

  fit_from <- bad_start - lubridate::days(SA_FIT_WINDOW_DAYS)

  # -- 1. SA on the 5-minute grid --------------------------------------------
  # na.approx()'s maxgap is a row count, not a duration -- convert assuming
  # the fixed 5-min PLS interval.
  sa_max_gap_rows <- ceiling(SA_MAX_INTERP_MINS / 5)

  pls <- pls |> left_join(sa, by = "measurement_time")

  pls$stage_sa_grid <- if (sum(!is.na(pls$stage_sa)) >= 2) {
    zoo::na.approx(pls$stage_sa, x = pls$measurement_time, na.rm = FALSE,
                   maxgap = sa_max_gap_rows)
  } else {
    NA_real_
  }

  # -- 2. Fit on the trusted window just before the bad period --------------
  sa_fit_data <- pls |>
    filter(origin == "raw", !is.na(value_qc), !is.na(stage_sa_grid),
           measurement_time >= fit_from, measurement_time < bad_start)

  if (nrow(sa_fit_data) < 100) {
    warning("Only ", nrow(sa_fit_data), " concurrent SA/PLS3 points in the ",
            SA_FIT_WINDOW_DAYS, "-day window before ", format(bad_start, "%Y-%m-%d"),
            " -- SA reconstruction NOT applied; bad_data rows stay MV.")
    pls <- pls |> select(-any_of(c("stage_sa", "stage_sa_grid")))
  } else {
    sa_fit  <- lm(value_qc ~ splines::ns(stage_sa_grid, df = SA_FIT_SPLINE_DF),
                  data = sa_fit_data)
    sa_r2   <- summary(sa_fit)$r.squared
    sa_rmse <- sqrt(mean(residuals(sa_fit)^2))

    message("SA -> PLS3 reconstruction fit: ", nrow(sa_fit_data), " points, ",
            format(fit_from, "%Y-%m-%d"), " to ", format(bad_start, "%Y-%m-%d"))
    message("  natural spline df = ", SA_FIT_SPLINE_DF,
            " | R2 = ", round(sa_r2, 4), " | residual RMSE = ", round(sa_rmse, 4), " m")
    if (sa_r2 < 0.9) {
      warning("SA -> PLS3 fit R2 is only ", round(sa_r2, 3),
              " -- inspect sa_recon_plot before trusting the reconstruction ",
              "(try a different SA_FIT_WINDOW_DAYS or SA_FIT_SPLINE_DF).")
    }

    # note if bad-window SA falls outside the fitted SA range (extrapolation)
    sa_bad <- pls$stage_sa_grid[pls$origin == "bad_data"]
    if (any(!is.na(sa_bad))) {
      fit_lo <- min(sa_fit_data$stage_sa_grid); fit_hi <- max(sa_fit_data$stage_sa_grid)
      if (min(sa_bad, na.rm = TRUE) < fit_lo || max(sa_bad, na.rm = TRUE) > fit_hi) {
        message("  NOTE: SA in the bad window spans [",
                round(min(sa_bad, na.rm = TRUE), 3), ", ", round(max(sa_bad, na.rm = TRUE), 3),
                "] m vs a fitted SA range of [", round(fit_lo, 3), ", ", round(fit_hi, 3),
                "] m -- values outside that range are linear extrapolation of the relationship.")
      }
    }

    pls$sa_recon <- predict(sa_fit, newdata = pls, na.action = na.pass)

    pls <- pls |>
      mutate(
        recon_ok    = origin == "bad_data" & !is.na(sa_recon),
        value_qc    = if_else(recon_ok, sa_recon, value_qc),
        fill_method = if_else(recon_ok, "sa_recon", fill_method)
      )

    n_recon    <- sum(pls$fill_method == "sa_recon", na.rm = TRUE)
    n_bad_left <- sum(pls$origin == "bad_data" & is.na(pls$value_qc))
    message("  reconstructed ", n_recon, " rows from SA; ",
            n_bad_left, " bad rows have no SA coverage (stay MV)")

    sa_grid_seq <- seq(min(sa_fit_data$stage_sa_grid), max(sa_fit_data$stage_sa_grid),
                       length.out = 200)
    sa_recon_plot <- tibble(
        stage_sa_grid = sa_grid_seq,
        pls3_pred     = predict(sa_fit, newdata = tibble(stage_sa_grid = sa_grid_seq))
      ) |>
      ggplot(aes(stage_sa_grid, pls3_pred)) +
      geom_point(data = sa_fit_data, aes(y = value_qc), alpha = 0.15, size = 0.5) +
      geom_line(colour = "firebrick", linewidth = 0.9) +
      labs(
        x = "SA sensor stage (m)", y = "PLS3 stage (m)",
        title = glue("SA -> PLS3 relationship (ns df {SA_FIT_SPLINE_DF}) -- R2 {round(sa_r2, 3)}"),
        subtitle = glue("Fit window {format(fit_from, '%Y-%m-%d')} to {format(bad_start, '%Y-%m-%d')}")
      )

    pls <- pls |> select(-any_of(c("stage_sa", "stage_sa_grid", "sa_recon", "recon_ok")))
  }
}


# -----------------------------------------------------------------------------
# 6. Gap filling -- Tier 2: spline interpolation
# -----------------------------------------------------------------------------
# Fills anything left after Tier 1, within MAX_FILL_GAP_MINS. bad_data rows
# excluded -- only ever rebuilt via SA in 5b.
pls <- pls |>
  mutate(
    is_gap = is.na(value_qc) & origin != "bad_data",
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
    force_fill = nrow(FORCE_FILL_WINDOWS) > 0 &&
      any(start <= FORCE_FILL_WINDOWS$end & end >= FORCE_FILL_WINDOWS$start)
  ) |>
  ungroup()

fillable_ids <- gap_info |> filter(gap_mins <= MAX_FILL_GAP_MINS | force_fill) |> pull(gap_id)

if (any(gap_info$force_fill)) {
  message(sum(gap_info$force_fill), " gap(s) manually forced fillable via FORCE_FILL_WINDOWS ",
          "despite exceeding MAX_FILL_GAP_MINS")
}

message(nrow(gap_info |> filter(gap_mins <= MAX_FILL_GAP_MINS | force_fill)),
        " remaining gaps duration-eligible for spline, ",
        nrow(gap_info |> filter(gap_mins > MAX_FILL_GAP_MINS & !force_fill)),
        " too long -- left as NA")

pls <- pls |>
  left_join(gap_info, by = "gap_id") |>
  mutate(
    value_for_spline = if_else(is_gap & !(gap_id %in% fillable_ids), NA_real_, value_qc),
    value_splined    = na.spline(value_for_spline, na.rm = FALSE),
    fill_method = if_else(is_gap & gap_id %in% fillable_ids, "spline", fill_method),
    value_qc    = if_else(is_gap & gap_id %in% fillable_ids, value_splined, value_qc)
  )

# na.spline() can still return NA with no bracketing data (record edges) --
# downgrade those instead of shipping a false "filled" flag.
n_before_validation <- sum(pls$fill_method == "spline" & is.na(pls$value_qc), na.rm = TRUE)

pls <- pls |>
  mutate(
    fill_method = if_else(fill_method == "spline" & is.na(value_qc), NA_character_, fill_method)
  ) |>
  select(-is_gap, -gap_id, -gap_mins, -start, -end, -force_fill, -value_for_spline, -value_splined)

if (n_before_validation > 0) {
  message("WARNING -- ", n_before_validation,
          " rows flagged as spline-filled but na.spline() returned NA. Downgraded to unfilled.")
}


# -----------------------------------------------------------------------------
# 7. Combine origin + fill_method into one qc_flag label
# -----------------------------------------------------------------------------
pls <- pls |>
  mutate(
    qc_flag = case_when(
      origin == "raw"                                       ~ "raw",
      origin == "flatline"                                   ~ "flagged_flatline",
      fill_method == "sa"                                    ~ "gf_sa",
      fill_method == "sa_recon"                               ~ "recon_sa",
      fill_method == "spline" & origin == "transmission_gap" ~ "gf_spline",
      fill_method == "spline" & origin == "spike"            ~ "replaced_spline",
      fill_method == "spline" & origin == "range_fail"       ~ "replaced_range",
      origin == "bad_data"                                   ~ "bad_data",
      TRUE                                                    ~ paste0("unfilled_", origin)
    )
  )

message("\n--- QC flag summary ---")
pls |> count(qc_flag) |> print(n = Inf)


# -----------------------------------------------------------------------------
# 7b. Verify gap-fill flags actually line up with real transmission gaps
# -----------------------------------------------------------------------------
# Should report 0 mismatches; if not, something upstream is mislabeling points.
gap_verification <- qc_verify_gap_fill(pls_raw, pls, measurement_time, value, qc_flag)
gap_verification$gap_audit  # one row per real gap, showing how it was resolved
# gap_verification$mismatches would list any problem points, if present


# -----------------------------------------------------------------------------
# 7c. Before/after summary stats, and a check for unexpected missing values
# -----------------------------------------------------------------------------
message("\n--- Before/after summary: raw vs QC'd ---")
print(qc_compare_summary(pls, value, value_qc))

message("\n--- Missing-value check by flag ---")
print(qc_check_missing(pls, value_qc, qc_flag))


# -----------------------------------------------------------------------------
# 8. Translate to the upload flag convention
# -----------------------------------------------------------------------------
# AV = accepted, AV:EV = accepted/estimated, PV = flagged only (unmodified),
# MV = missing/unfillable
#
# quality_level: 2 = raw + unfilled/unfillable (includes bad_data), 3 =
# gap-filled/estimated (fill_method not NA)
pls_upload <- pls |>
  mutate(
    quality_level = if_else(!is.na(fill_method), 3, 2),
    qc_flag_code = case_when(
      qc_flag == "gf_sa"           ~ "AV:EV: Transmission gap filled using SA sensor relationship",
      qc_flag == "recon_sa"        ~ glue("AV:EV: Primary sensor failed; series reconstructed from SA sensor via fitted SA->PLS3 relationship (natural spline, {SA_FIT_WINDOW_DAYS}-day fit window)"),
      qc_flag == "gf_spline"       ~ "AV:EV: Transmission gap filled via spline interpolation",
      qc_flag == "replaced_spline" ~ "AV:EV: Spike removed and corrected via spline interpolation",
      qc_flag == "bad_data"        ~ "MV: Primary sensor failed and no SA coverage to reconstruct from",
      qc_flag == "replaced_range"  ~ glue("AV:EV: Value outside plausible range [{RANGE_MIN}, {RANGE_MAX}] m, corrected via spline interpolation"),
      qc_flag == "flagged_flatline" ~ glue("PV: Flatlined for >= {FLATLINE_WINDOW_DAYS} days (low flow or icing potential) -- value NOT modified, flagged for review only"),
      str_starts(qc_flag, "unfilled") ~ glue("MV: No value available ({str_remove(qc_flag, 'unfilled_')}, gap > {MAX_FILL_GAP_MINS} min or no bracketing data)"),
      TRUE                             ~ "AV"
    )
  )

# -- Sanity check: every row must have a resolved quality_level and qc_flag --
# Catches any qc_flag not covered above (would otherwise upload as NA).
missing_quality <- pls_upload |>
  filter(is.na(quality_level) | is.na(qc_flag) | is.na(qc_flag_code))

if (nrow(missing_quality) > 0) {
  warning(
    nrow(missing_quality), " row(s) have no quality_level and/or qc_flag_code -- ",
    "check case_when() in section 8 for a qc_flag value it doesn't cover. ",
    "First affected timestamps: ",
    paste(head(missing_quality$measurement_time, 10), collapse = ", "),
    if (nrow(missing_quality) > 10) ", ..." else ""
  )
  print(missing_quality)
} else {
  message("OK: all ", nrow(pls_upload), " rows have a quality_level and qc_flag.")
}


# -----------------------------------------------------------------------------
# 9. Build the columns required for upload
# -----------------------------------------------------------------------------
# avg carries the corrected/estimated value 
pls_for_db <- pls_upload |>
  transmute(
    measurement_time = strftime(measurement_time, "%Y-%m-%dT%H:%M:%S%z"),
    quality_level    = quality_level,
    qc_flag          = qc_flag_code,
    measurement_name = MEASUREMENT_NAME,
    qc_by            = QC_BY,
    recorded_time    = strftime(lubridate::now(), "%Y-%m-%dT%H:%M:%S%z"),
    avg              = value_qc
  )

glimpse(pls_for_db)

saveRDS(pls_for_db, "pls2_for_db_SSN703US_2017-11-13to2021-03-21.rds")

# -----------------------------------------------------------------------------
# 10. Visualize results
# -----------------------------------------------------------------------------
# print() explicit -- auto-print can get suppressed running top to bottom.
chunks <- qc_summarise_chunks(pls, measurement_time, qc_flag)
print(chunks, n = Inf)

print(qc_plot_review(pls, measurement_time, value, value_qc, qc_flag, chunks = chunks))

print(qc_plot_interactive(pls, measurement_time, value, value_qc, qc_flag))

# SA -> PLS3 fit diagnostic (only exists if a bad-data window was reconstructed)
if (exists("sa_recon_plot")) print(sa_recon_plot)


# -----------------------------------------------------------------------------
# 11. Upload (NOT run automatically -- review pls_for_db first)
# -----------------------------------------------------------------------------
# table_name <- sn_qc_table_name(SITE, "5minuteSamples")
# baseurl <- glue("api/sn/qc/{table_name}")
# window_size <- 1000
# for (i in 0:(nrow(pls_for_db) %/% window_size)) {
#   lb <- i * window_size + 1
#   ub <- min(nrow(pls_for_db), (i + 1) * window_size)
#   client$post(baseurl, pls_for_db[lb:ub, ])
# }


# =============================================================================
# On the AV/EV/MV convention itself
# =============================================================================
# Loses origin/fill_method detail and isn't self-documenting or easily
# filterable as free text. Kept origin/fill_method as the real QC record in
# `pls`; only collapsed to the AV/EV/MV string at upload, since that's what
# the schema expects. Cleaner fix would be separate origin/fill_method/
# quality_level/notes columns in the schema itself, if ever revisited.