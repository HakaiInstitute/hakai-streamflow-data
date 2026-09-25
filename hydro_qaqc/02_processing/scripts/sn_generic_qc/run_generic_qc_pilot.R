# =============================================================================
# run_generic_qc_pilot.R -- pilot test of the generalized sn/views download +
# QC + upload workflow, across 3 different measurement types
# =============================================================================
# Run from the PROJECT ROOT. Does NOT touch qc-functions.R, pls-workflow.R,
# or anything under stage_qc/ -- those stay as working fallbacks. This
# exercises the new, separate generic engine in sn_generic_qc/ end to end:
#   1. discover the exact component string for each variable (no memorized
#      site:view:component strings -- see sn_discovery.R)
#   2. download raw values (reusing sn_read_values(), unmodified, from
#      stage_qc_functions.R)
#   3. run the generic QC engine (qc_generic_pipeline.R::qc_run())
#   4. format for upload and run the guardrail (qc_generic_upload.R) --
#      NOTHING is uploaded (PATCH or POST) by this script; sn_post_qc() is
#      never called here.
#
# Pilot variables (chosen to cover 3 different kinds of measurement):
#   - Hecate:5minuteSamples  RH        (relative humidity, % -- met sensor)
#   - Hecate:5minuteSamples  Air Temp  (deg C -- met sensor)
#   - SSN626US:5minuteSamples PLS_Lvl  (m -- stage-shaped, but a station the
#     existing stage_qc/ pipeline has never been pointed at)
#
# CAVEAT -- thresholds below (range_min/max, flatline_tolerance) are
# PLACEHOLDERS, same status as RANGE_MIN/MAX/FLATLINE_TOLERANCE_M in
# pls-workflow.R ("unconfirmed as an actual QC threshold vs. sensor spec")
# -- tune them against real data before trusting the flag counts for
# anything beyond "does the pipeline run end to end."
#
# CAVEAT -- this pilot targets the sn/qc upload shape only (qc_format_upload/
# qc_validate_upload/sn_post_qc). upload_ready.R's Telemetry-Network CSV
# format (WtrLvl<gen><site>-prefixed columns) is NOT generalized here --
# Hakai's naming convention for that format for non-level measurements isn't
# confirmed anywhere in this repo.
# =============================================================================

source("02_processing/scripts/stage_qc/stage_qc_functions.R")
source("02_processing/scripts/sn_generic_qc/sn_discovery.R")
source("02_processing/scripts/sn_generic_qc/qc_generic_pipeline.R")
source("02_processing/scripts/sn_generic_qc/qc_generic_upload.R")

QC_BY <- "emily.haughton@hakai.org"


# -----------------------------------------------------------------------------
# 0. Connect
# -----------------------------------------------------------------------------
client <- sn_connect()


# -----------------------------------------------------------------------------
# 1. Pilot configuration -- one row per variable
# -----------------------------------------------------------------------------
# component_pattern: a hint regex, NOT the exact component string -- resolved
# via sn_find_components() below so nothing here has to be memorized/guessed.
PILOT <- list(
  hecate_rh = list(
    site = "Hecate", view = "5minuteSamples", component_pattern = "hum",
    start_date = "2015-01-01", end_date = format(Sys.Date(), "%Y-%m-%d"),
    range_min = 0, range_max = 100,        # RH is bounded 0-100 by definition
    flatline_tolerance = NA_real_          # not set -- adjust once real data is seen
  ),
  hecate_air_temp = list(
    site = "Hecate", view = "5minuteSamples", component_pattern = "temp",
    start_date = "2015-01-01", end_date = format(Sys.Date(), "%Y-%m-%d"),
    range_min = -20, range_max = 40,       # PLACEHOLDER -- unconfirmed vs sensor spec
    flatline_tolerance = NA_real_
  ),
  ssn626us_pls_lvl = list(
    site = "SSN626US", view = "5minuteSamples", component_pattern = "PLS",
    start_date = "2015-01-01", end_date = format(Sys.Date(), "%Y-%m-%d"),
    range_min = 0.02, range_max = 2.8,     # PLACEHOLDER, same numbers as SSN703 --
    flatline_tolerance = 0.005             # unconfirmed for this station specifically
  )
)


# -----------------------------------------------------------------------------
# 2. Run each pilot variable: discover -> download -> QC -> format -> validate
# -----------------------------------------------------------------------------
run_pilot_variable <- function(client, cfg, label) {
  message("\n=============================================================")
  message("  ", label, "  (", cfg$site, ":", cfg$view, ", pattern '", cfg$component_pattern, "')")
  message("=============================================================")

  # -- discover the exact component string --
  found <- sn_find_components(client, cfg$site, cfg$view, cfg$component_pattern)
  if (nrow(found) == 0) {
    message("No component matched for ", label, " -- skipping.")
    return(NULL)
  }
  if (nrow(found) > 1) {
    message("Multiple components matched '", cfg$component_pattern, "' for ", label,
            ": ", paste(found$component, collapse = ", "),
            " -- using the first (", found$component[1], "). Narrow component_pattern if this is wrong.")
  }
  component <- found$component[1]
  message("Resolved component: ", component)

  # -- download (reusing sn_read_values() from stage_qc_functions.R) --
  raw <- sn_read_values(client, cfg$site, cfg$view, component, cfg$start_date, cfg$end_date) |>
    select(measurement_time, value)

  if (nrow(raw) == 0) {
    message("No data returned for ", label, " in this date range -- skipping.")
    return(NULL)
  }

  # -- QC (no reference series for any of the 3 pilot variables) --
  params <- qc_default_params()
  params$range_min           <- cfg$range_min
  params$range_max           <- cfg$range_max
  params$flatline_tolerance  <- cfg$flatline_tolerance

  qc <- qc_run(raw, reference = NULL, params = params)
  print(qc_plot_diagnostic(qc, measurement_time, value_qc, flag = qc_flag,
                            title = label, value_label = component))

  # -- format for upload + validate (NOT posted) --
  upload <- qc_format_upload(qc, measurement_name = component, qc_by = QC_BY, params = params)
  glimpse(upload)

  problems <- qc_validate_upload(upload, qc)
  print(problems, n = Inf)

  list(component = component, raw = raw, qc = qc, upload = upload, problems = problems)
}

results <- imap(PILOT, ~ run_pilot_variable(client, .x, .y))


# -----------------------------------------------------------------------------
# 3. Summary across all 3
# -----------------------------------------------------------------------------
message("\n--- Pilot summary ---")
for (label in names(results)) {
  r <- results[[label]]
  if (is.null(r)) {
    message(label, ": SKIPPED (no data/component found)")
  } else {
    message(label, " (", r$component, "): ", nrow(r$qc), " rows QC'd, ",
            nrow(r$problems), " upload-validation problem(s)")
  }
}
