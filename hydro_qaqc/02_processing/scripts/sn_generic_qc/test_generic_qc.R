# =============================================================================
# test_generic_qc.R -- manual, one-variable-at-a-time test of the generalized
# sn/views download + QC + upload workflow
# =============================================================================
# Run from the PROJECT ROOT, top to bottom (or line by line / chunk by
# chunk in RStudio -- no wrapper functions, nothing hidden). Edit SITE /
# VIEW / COMPONENT_PATTERN in step 2 and re-run to test a different
# site/measurement. Does not touch qc-functions.R, pls-workflow.R, or
# stage_qc/ -- those still work as before.
#
# Nothing in this script POSTs anything -- step 8 only validates.
# =============================================================================

source("02_processing/scripts/stage_qc/stage_qc_functions.R")
source("02_processing/scripts/sn_generic_qc/sn_discovery.R")
source("02_processing/scripts/sn_generic_qc/qc_generic_pipeline.R")
source("02_processing/scripts/sn_generic_qc/qc_generic_upload.R")


# -----------------------------------------------------------------------------
# 1. Connect
# -----------------------------------------------------------------------------
client <- sn_connect()


# -----------------------------------------------------------------------------
# 2. EDIT THESE for whatever you're testing right now
# -----------------------------------------------------------------------------
SITE              <- "SSN626US"
VIEW              <- "5minuteSamples"
COMPONENT_PATTERN <- "pls"          # a hint, not the exact string -- resolved in step 3

START_DATE <- "2024-01-01"
END_DATE   <- "2025-01-01"

QC_BY <- "emily.haughton@hakai.org"


# -----------------------------------------------------------------------------
# 3. Discover the exact component string (no memorizing needed)
# -----------------------------------------------------------------------------
# If this doesn't find what you expect, try sn_find_views(client, "<site pattern>")
# first to confirm the site/view exist at all, or loosen/tighten COMPONENT_PATTERN.
found <- sn_find_components(client, SITE, VIEW, COMPONENT_PATTERN)
#sn_find_views(client, "SSN626US")
print(found)

# Defaults to the first match. If more than one row came back and it's not
# the one you want, set COMPONENT explicitly instead (e.g. found$component[2]).
if (nrow(found) > 1) {
  message("Multiple components matched '", COMPONENT_PATTERN, "': ",
          paste(found$component, collapse = ", "),
          " -- using the first (", found$component[1], "). Set COMPONENT explicitly if this is wrong.")
}
COMPONENT <- found$component[5]
message("Using component: ", COMPONENT)


# -----------------------------------------------------------------------------
# 4. Download raw values + Min/Max/Std burst stats in one call
# -----------------------------------------------------------------------------
# sn_read_measurement() automatically finds and pulls whichever of
# COMPONENT/_Avg/_Min/_Max/_Std actually exist for this site:view -- prints
# which it found. Any stat the API doesn't expose comes back NA (the column
# still exists), it isn't dropped. Feed this straight into qc_run() in step 6
# -- value_min/value_max/value_std are carried through natively now, no
# separate join step needed before qc_format_upload(include_stats = TRUE).
raw_stats <- sn_read_measurement(client, SITE, VIEW, COMPONENT, START_DATE, END_DATE)

nrow(raw_stats)
range(raw_stats$measurement_time)
summary(raw_stats$value)


# -----------------------------------------------------------------------------
# 5. EDIT THESE -- QC thresholds for whatever COMPONENT actually is.
#    NA = skip that check. There is no universal default across level/RH/
#    temperature/etc, so set these deliberately each time you change SITE/
#    COMPONENT above, based on what summary(raw_stats$value) just showed you.
# -----------------------------------------------------------------------------
params <- qc_default_params()
params$range_min          <- 0   # e.g. 0 for RH
params$range_max          <- 2  # e.g. 100 for RH
params$flatline_tolerance <- 0.005   # e.g. 0.005 for a stage-like variable in m

# Only set this if you have a second, correlated sensor at the same site to
# gap-fill from (like SA for SSN703 stage). Otherwise leave reference = NULL
# in step 6 -- gap-fill tiers 1/1b just skip cleanly.
reference <- NULL


# -----------------------------------------------------------------------------
# 6. Run the generic QC engine -- Min/Max/Std ride along automatically since
#    raw_stats (not a measurement_time/value-only tibble) is passed in
# -----------------------------------------------------------------------------
qc <- qc_run(raw_stats, reference = reference, params = params)

qc |> count(qc_flag)
print(qc_plot_diagnostic(qc, measurement_time, value_qc, flag = qc_flag,
                          title = paste(SITE, COMPONENT), value_label = COMPONENT))


# -----------------------------------------------------------------------------
# 7. Format for upload (NOT posted) -- quality_level, qc_flag, unesco_q_level,
#    measurement_name, min, max, std all included via include_stats = TRUE.
#    That extended shape is NOT confirmed against the live sn/qc endpoint
#    (only the default 7-column shape is) -- drop include_stats = TRUE below
#    to fall back to the confirmed shape if you're not ready to test it live.
# -----------------------------------------------------------------------------
upload <- qc_format_upload(qc, measurement_name = COMPONENT, qc_by = QC_BY,
                            params = params, include_stats = TRUE)
glimpse(upload)


# -----------------------------------------------------------------------------
# 8. Validate before ever uploading anything
# -----------------------------------------------------------------------------
# Pass client + a table_name to also cross-check measurement_name against
# that table's real QC history (uncomment once you know the table name --
# sn_qc_table_name(SITE, VIEW) derives it the same way the rest of the repo
# does, though it may not exist yet for a brand-new site/variable):
#   table_name <- sn_qc_table_name(SITE, VIEW)
#   problems <- qc_validate_upload(upload, qc, client = client, table_name = table_name)
problems <- qc_validate_upload(upload, qc)
print(problems, n = Inf)

if (nrow(problems) == 0) {
  message("Clean -- upload tibble is ready for review. Still NOT posted by this script.")
} else {
  message(nrow(problems), " problem(s) above -- fix before considering upload.")
}


# -----------------------------------------------------------------------------
# 9. Upload -- NOT run. Uncomment only when you're actually ready, and only
#    after `problems` above is 0 rows (sn_post_qc() enforces this anyway).
# -----------------------------------------------------------------------------
# table_name <- sn_qc_table_name(SITE, VIEW)
# sn_post_qc(client, upload, qc, table_name = table_name)


