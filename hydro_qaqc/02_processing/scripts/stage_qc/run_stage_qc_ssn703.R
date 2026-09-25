# =============================================================================
# run_stage_qc_ssn703.R -- SSN703 stage QC driver
# =============================================================================
# Run from the PROJECT ROOT:
#   source("02_processing/scripts/stage_qc/run_stage_qc_ssn703.R")
#
# Replaces: 03_offset_calculation.R, 04_stage_qc.R, 04_stage_qc_v2.R,
#           05_stage_output.R, 703_stage_qc*.R (all four),
#           pls_qc_for_upload_example.R, and pls-workflow.R's bottom half.
#
# For each SSN703 pressure-transducer generation (a/b/c/d) this:
#   1. loads the raw primary series (local CSV by default; API optional)
#   2. loads the SA reference series (local CSV by default; API optional)
#   3. runs stage_qc_pipeline.R::stage_qc_run() with per-generation params,
#      including the DATUM OFFSET from 03_docs/metadata/offsets.csv
#   4. writes the analysis-ready per-sensor QC CSV (schema unchanged from the
#      old 05_stage_output.R, so 10_discharge.R etc. keep working)
#   5. writes the Hakai DB upload RDS (pls{,2,3,4}_for_db_*.rds)
#   6. writes review plots
# Then stitches the per-RC CSVs (RC1 = a then b, RC2 = c, RC3 = d).
#
# THE DATUM OFFSET (see 03_docs/metadata/offsets.csv and
# memory/mk-rc1-stage-provenance.md):
#   ssn703_a is MK's reference datum -> offset 0.
#   ssn703_b reads ~1.9 cm high relative to that datum -> offset -0.019 m,
#     applied across its whole RC1 record so the a->b transition is smooth.
#   ssn703_c / ssn703_d are location breaks (loc_2 / loc_3) and start their
#     own rating-curve periods -> no offset.
# =============================================================================

source("02_processing/scripts/stage_qc/stage_qc_functions.R")
source("02_processing/scripts/stage_qc/stage_qc_pipeline.R")


# -----------------------------------------------------------------------------
# 0. Global config
# -----------------------------------------------------------------------------
STATION      <- "SSN703"
RAW_DIR      <- "01_raw/SSN703"
META_DIR     <- "03_docs/metadata"
OUT_SENSOR   <- "04_outputs/per_sensor"
OUT_RC       <- "04_outputs/per_rc"
PLOT_DIR     <- "02_processing/plots"
DB_RDS_DIR   <- "."                       # pls*_for_db_*.rds live in the repo root

for (d in c(OUT_SENSOR, OUT_RC, PLOT_DIR)) dir.create(d, showWarnings = FALSE, recursive = TRUE)

# Primary-series source: "csv" (per-sensor files in 01_raw, unambiguous,
# offline) or "api" (live Hakai pull -- one continuous PLS_Lvl series).
PRIMARY_SOURCE <- "csv"
# SA reference source: "csv" (01_raw/SSN703/ssn703_sa.csv) or "api"
# (SA_WTS703_PT / SensorDepth_Avg) or "none".
SA_SOURCE      <- "csv"

QC_BY <- "emily.haughton@hakai.org"

# API window (only used when a *_SOURCE is "api")
API_START <- "2014-08-01"
API_END   <- format(Sys.Date(), "%Y-%m-%d")


# -----------------------------------------------------------------------------
# 1. Metadata: registry + offsets
# -----------------------------------------------------------------------------
registry <- stage_read_registry(file.path(META_DIR, "sensor_registry.csv"), STATION)

offsets <- read_csv(file.path(META_DIR, "offsets.csv"), show_col_types = FALSE) |>
  filter(station_id == STATION)

message("Offsets on record for ", STATION, ":")
offsets |> select(sensor_id_failing, sensor_id_reference, offset_m, offset_method) |> print()

#' Datum-offset window for one sensor, in the shape stage_qc_run() expects.
#' offsets.csv records the value to ADD to `sensor_id_failing` to bring it
#' onto `sensor_id_reference`'s datum; here we apply it across that sensor's
#' whole deployment window (RC1 sensors are co-located at loc_1).
offset_windows_for <- function(sensor_id, dep_start, dep_end) {
  row <- offsets |> filter(sensor_id_failing == sensor_id)
  if (nrow(row) == 0) {
    return(tibble(start = as.POSIXct(character()),
                  end   = as.POSIXct(character()),
                  offset_m = numeric()))
  }
  tibble(start = dep_start, end = dep_end, offset_m = row$offset_m[1])
}


# -----------------------------------------------------------------------------
# 2. Per-generation config, built from the registry
# -----------------------------------------------------------------------------
# gen         = PLS generation short code -> DB upload measurement_name context
# sensor_id   = raw-file stem / registry key
# location_id, rating_curve_period from the registry
# dep_start / dep_end / bad windows from the registry
# thresholds  = per-generation QC thresholds (start from stage_qc_default_params)

GENERATIONS <- c(a = "PLS", b = "PLS2", c = "PLS3", d = "PLS4")

build_config <- function(letter) {
  sid <- paste0("ssn703_", letter)
  r   <- registry |> filter(site_id == sid)
  stopifnot(nrow(r) == 1)

  dep_start <- as.POSIXct(r$date_start, tz = "UTC", format = "%Y-%m-%dT%H:%M:%SZ")
  dep_end   <- if (is.na(r$date_end)) as.POSIXct(NA) else
    as.POSIXct(r$date_end, tz = "UTC", format = "%Y-%m-%dT%H:%M:%SZ")

  bad <- if (is.na(r$bad_data_start)) {
    tibble(start = as.POSIXct(character()), end = as.POSIXct(character()))
  } else {
    tibble(
      start = as.POSIXct(r$bad_data_start, tz = "UTC", format = "%Y-%m-%dT%H:%M:%SZ"),
      end   = if (is.na(r$bad_data_end)) as.POSIXct(NA) else
        as.POSIXct(r$bad_data_end, tz = "UTC", format = "%Y-%m-%dT%H:%M:%SZ")
    )
  }

  params <- stage_qc_default_params()
  params$offset_windows   <- offset_windows_for(sid, dep_start, dep_end)
  params$bad_data_windows <- bad

  # -- per-generation threshold overrides (tune from the diagnostics) --
  if (letter == "d") {
    # PLS4 / RC3: SA sensor was retired 2025-05, and the Oct-Nov 2023
    # bad-data window is the main reconstruction target.
    params$sa_fit_window_days <- 365
  }

  list(
    gen              = unname(GENERATIONS[letter]),
    letter           = letter,
    sensor_id        = sid,
    station_id       = r$station_id,
    location_id      = r$location_id,
    rc_period        = r$rating_curve_period,
    dep_start        = dep_start,
    dep_end          = dep_end,
    # network component / QC measurement_name for this generation:
    # PLS_Lvl / PLS2_Lvl / PLS3_Lvl / PLS4_Lvl
    measurement_name = paste0(unname(GENERATIONS[letter]), "_Lvl"),
    params           = params
  )
}

CONFIGS <- map(names(GENERATIONS), build_config)
names(CONFIGS) <- names(GENERATIONS)


# -----------------------------------------------------------------------------
# 3. Loaders
# -----------------------------------------------------------------------------
api_client <- NULL   # lazily connected only if a source is "api"

get_client <- function() {
  if (is.null(api_client)) api_client <<- sn_connect()
  api_client
}

load_primary <- function(cfg) {
  # Each PT generation is a distinct component in the network:
  #   ssn703_a -> PLS_Lvl, _b -> PLS2_Lvl, _c -> PLS3_Lvl, _d -> PLS4_Lvl
  component <- cfg$measurement_name
  if (PRIMARY_SOURCE == "csv") {
    stage_read_raw_csv(cfg$sensor_id, RAW_DIR, variable = "stage_avg") |>
      select(measurement_time, value)
  } else {
    sn_read_values(get_client(), "SSN703US", "5minuteSamples", component,
                   API_START, API_END) |>
      filter(variable == component) |>
      select(measurement_time, value)
  }
}

load_sa <- function() {
  if (SA_SOURCE == "none") return(NULL)
  if (SA_SOURCE == "csv") {
    stage_read_raw_csv("ssn703_sa", RAW_DIR, variable = "stage_avg") |>
      select(measurement_time, value)
  } else {
    sn_read_values(get_client(), "SA_WTS703_PT", "5minuteSamples",
                   "SensorDepth_Avg", API_START, API_END) |>
      filter(variable == "SensorDepth_Avg") |>
      select(measurement_time, value)
  }
}

sa_raw <- load_sa()


# -----------------------------------------------------------------------------
# 4. Helpers: output formatting
# -----------------------------------------------------------------------------
water_year_label <- function(t_pst) {
  y <- lubridate::year(t_pst); m <- lubridate::month(t_pst)
  if_else(m >= 10, paste0(y, "-", y + 1), paste0(y - 1, "-", y))
}

# Map the pipeline's rich flag vocabulary down to the flag values the
# downstream discharge scripts (10_discharge.R, 11_discharge_summary.R,
# upload_ready.R) already understand. The full label is kept in
# `qc_flag_detail`.
DOWNSTREAM_FLAG <- c(
  raw               = "raw",
  offset_corrected  = "raw",             # measured value, datum-shifted only
  flagged_flatline  = "raw",             # flag-only, value not modified
  gf_sa             = "gf_sa",
  recon_sa          = "gf_sa",
  gf_spline         = "gf_spline",
  replaced_range    = "gf_spline",
  bad_data          = "bad_data"
)
downstream_flag <- function(qc_flag) {
  out <- unname(DOWNSTREAM_FLAG[qc_flag])
  out[is.na(out) & str_starts(qc_flag, "unfilled")] <- "unfilled"
  out
}

#' Build the per-sensor QC CSV (schema matches the old 05_stage_output.R)
to_per_sensor <- function(qc, cfg) {
  qc |>
    mutate(
      t_pst           = lubridate::with_tz(measurement_time, "Etc/GMT+8"),
      qc_flag_full    = qc_flag,                    # keep the engine's label
      qc_flag_simple  = downstream_flag(qc_flag)    # collapse for 10_discharge.R et al.
    ) |>
    transmute(
      station_id          = cfg$station_id,
      location_id         = cfg$location_id,
      rating_curve_period = cfg$rc_period,
      timestamp           = strftime(measurement_time, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
      water_year          = water_year_label(t_pst),
      stage_corrected     = value_qc,
      offset_applied      = coalesce(offset_applied_m, 0),
      qc_flag             = qc_flag_simple,
      qc_flag_detail      = qc_flag_full,
      site_id             = cfg$sensor_id
    )
}

#' Build the Hakai DB upload tibble (from pls-workflow.R sections 8-9)
to_db_upload <- function(qc, cfg) {
  p <- cfg$params
  qc |>
    mutate(
      quality_level = if_else(!is.na(fill_method) | qc_flag == "offset_corrected", 3, 2),
      qc_flag_code = case_when(
        qc_flag == "offset_corrected" ~ glue("AV:EV: Stage offset of {offset_applied_m} m applied to align sensor to the loc_1 (ssn703_a) datum"),
        qc_flag == "gf_sa"            ~ "AV:EV: Transmission gap filled using SA sensor relationship",
        qc_flag == "recon_sa"         ~ glue("AV:EV: Primary sensor failed; series reconstructed from SA sensor via fitted SA->primary relationship (natural spline, {p$sa_fit_window_days}-day fit window)"),
        qc_flag == "gf_spline"        ~ "AV:EV: Transmission gap filled via spline interpolation",
        qc_flag == "replaced_range"   ~ glue("AV:EV: Value outside plausible range [{p$range_min}, {p$range_max}] m, corrected via spline interpolation"),
        qc_flag == "bad_data"         ~ "MV: Primary sensor failed and no SA coverage to reconstruct from",
        qc_flag == "flagged_flatline" ~ glue("PV: Flatlined for >= {p$flatline_window_days} days (low flow or icing potential) -- value NOT modified, flagged for review only"),
        str_starts(qc_flag, "unfilled") ~ glue("MV: No value available ({str_remove(qc_flag, 'unfilled_')}, gap > {p$max_fill_gap_mins} min or no bracketing data)"),
        TRUE                            ~ "AV"
      )
    ) |>
    transmute(
      measurement_time = strftime(measurement_time, "%Y-%m-%dT%H:%M:%S%z"),
      quality_level,
      qc_flag          = qc_flag_code,
      measurement_name = cfg$measurement_name,   # PLS_Lvl / PLS2_Lvl / PLS3_Lvl / PLS4_Lvl
      qc_by            = QC_BY,
      recorded_time    = strftime(lubridate::now(), "%Y-%m-%dT%H:%M:%S%z"),
      avg              = value_qc
    )
}


# -----------------------------------------------------------------------------
# 5. Run each generation
# -----------------------------------------------------------------------------
qc_results  <- list()
per_sensor  <- list()

for (letter in names(CONFIGS)) {
  cfg <- CONFIGS[[letter]]
  message("\n=============================================================")
  message("  ", cfg$sensor_id, "  (", cfg$gen, ", ", cfg$rc_period, ", ", cfg$location_id, ")")
  message("=============================================================")

  raw <- load_primary(cfg)
  if (!is.na(cfg$dep_start)) raw <- raw |> filter(measurement_time >= cfg$dep_start)
  if (!is.na(cfg$dep_end))   raw <- raw |> filter(measurement_time <= cfg$dep_end)

  qc <- stage_qc_run(raw, sa = sa_raw, params = cfg$params)
  qc_results[[letter]] <- qc

  ps <- to_per_sensor(qc, cfg)
  per_sensor[[letter]] <- ps

  ps_path <- file.path(OUT_SENSOR, paste0("ssn703_", cfg$sensor_id, "_stage_qc.csv"))
  write_csv(ps, ps_path)
  message("Saved: ", ps_path, " (", nrow(ps), " rows)")

  db <- to_db_upload(qc, cfg)
  rng <- range(as.Date(qc$measurement_time))
  gen_num <- if (cfg$gen == "PLS") "" else sub("PLS", "", cfg$gen)
  db_path <- file.path(DB_RDS_DIR, glue("pls{gen_num}_for_db_SSN703US_{rng[1]}to{rng[2]}.rds"))
  saveRDS(db, db_path)
  message("Saved: ", db_path)

  # -- review artefacts --
  print(qc_check_missing(qc, value_qc, qc_flag))
  # only transmission-gap fills are expected to sit inside a raw NA gap;
  # recon_sa sits inside a bad-data window (raw non-NA) by design, so it is
  # not checked here.
  gv <- qc_verify_gap_fill(raw, qc, measurement_time, value, qc_flag,
                           fill_flags = c("gf_spline", "gf_sa"))

  # Clip the review-plot y-axis to the QC'd stage range. The grey "raw" line
  # includes range-fail / bad-data excursions in the raw value column, which
  # otherwise blow the y-axis out and flatten the real signal to a line.
  yr <- range(qc$value_qc, na.rm = TRUE) + c(-0.05, 0.05)
  p_review <- qc_plot_review(qc, measurement_time, value, value_qc, qc_flag) +
    coord_cartesian(ylim = yr) +
    labs(title = glue("SSN703 {cfg$sensor_id} -- QC review (y clipped to QC'd range)"))
  ggsave(file.path(PLOT_DIR, glue("ssn703_{cfg$sensor_id}_qc_review.pdf")),
         p_review, width = 14, height = 6)

  if (!is.null(attr(qc, "sa_recon_plot"))) {
    ggsave(file.path(PLOT_DIR, glue("ssn703_{cfg$sensor_id}_sa_recon.pdf")),
           attr(qc, "sa_recon_plot"), width = 8, height = 6)
  }
}


# -----------------------------------------------------------------------------
# 6. Offset validation plot -- a/b transition on the corrected datum
# -----------------------------------------------------------------------------
ov_ab <- read_csv(file.path(META_DIR, "overlap_registry.csv"), show_col_types = FALSE) |>
  filter(station_id == STATION, sensor_id_failing == "ssn703_a")

ab_corrected <- bind_rows(
  qc_results$a |> mutate(site = "ssn703_a"),
  qc_results$b |> mutate(site = "ssn703_b")
) |>
  filter(measurement_time >= as.POSIXct(ov_ab$overlap_start[1], tz = "UTC",
                                        format = "%Y-%m-%dT%H:%M:%SZ"),
         measurement_time <= as.POSIXct(ov_ab$overlap_end[1], tz = "UTC",
                                        format = "%Y-%m-%dT%H:%M:%SZ"))

p_offset <- ggplot(ab_corrected, aes(measurement_time, value_qc, colour = site)) +
  geom_line(linewidth = 0.3, na.rm = TRUE, alpha = 0.85) +
  labs(title = "SSN703 -- ssn703_a / ssn703_b on the corrected (ssn703_a) datum",
       subtitle = glue("ssn703_b offset {offsets$offset_m[offsets$sensor_id_failing == 'ssn703_b']} m applied; ",
                       "transition should be smooth with no step at the ssn703_b install date"),
       x = NULL, y = "Stage corrected (m)", colour = NULL) +
  theme_bw() + theme(legend.position = "bottom")
ggsave(file.path(PLOT_DIR, "ssn703_offset_validation.pdf"), p_offset, width = 14, height = 6)
message("\nSaved: ", file.path(PLOT_DIR, "ssn703_offset_validation.pdf"))


# -----------------------------------------------------------------------------
# 7. Stitch per-RC files (chronological, de-overlapped)
# -----------------------------------------------------------------------------
# RC1 = ssn703_a up to the ssn703_b install, then ssn703_b through its end.
# RC2 = ssn703_c full deployment. RC3 = ssn703_d full deployment.
b_install <- CONFIGS$b$dep_start

rc1 <- bind_rows(
  per_sensor$a |> filter(as.POSIXct(timestamp, tz = "UTC", format = "%Y-%m-%dT%H:%M:%SZ") < b_install),
  per_sensor$b
) |>
  mutate(rating_curve_period = "RC1") |>
  arrange(timestamp)

rc2 <- per_sensor$c |> mutate(rating_curve_period = "RC2") |> arrange(timestamp)
rc3 <- per_sensor$d |> mutate(rating_curve_period = "RC3") |> arrange(timestamp)

for (rc in list(list("RC1", rc1), list("RC2", rc2), list("RC3", rc3))) {
  dup <- rc[[2]] |> count(timestamp) |> filter(n > 1)
  if (nrow(dup) > 0) warning(rc[[1]], ": ", nrow(dup), " duplicate timestamps at the sensor boundary")
  path <- file.path(OUT_RC, paste0("ssn703_", rc[[1]], "_stage_qc.csv"))
  write_csv(rc[[2]], path)
  message("Saved: ", path, " (", nrow(rc[[2]]), " rows)")
}


# -----------------------------------------------------------------------------
# 8. Summary
# -----------------------------------------------------------------------------
message("\n--- QC flag breakdown per RC (downstream vocabulary) ---")
bind_rows(rc1, rc2, rc3) |>
  count(rating_curve_period, qc_flag) |>
  pivot_wider(names_from = qc_flag, values_from = n, values_fill = 0) |>
  print()

message("\nDone. Next:")
message("  - review 02_processing/plots/ssn703_*_qc_review.pdf and ssn703_offset_validation.pdf")
message("  - rating curve work reads 04_outputs/per_rc/ and per_sensor/ from here")
message("  - DB upload: review the pls*_for_db_*.rds, then POST (see pls-workflow archive for the loop)")
