# =============================================================================
# PLS_Lvl QC + Upload Prep -- worked example
# =============================================================================
# Purpose:
#   Demonstrates spike detection, SA-relationship gap filling, and spline
#   gap filling, adapted from 04_stage_qc.R. Ends by building the columns
#   required for a Hakai DB QC upload (including the corrected value).
#
# SA sensor note: SSN703's SA (secondary/reference) sensor lives under its
# own separate site code in the API, SA_WTS703_PT (not a component under
# SSN703US), view "5minuteSamples". Its level-equivalent field is
# SensorDepth_Avg (pressure-transducer-derived depth -- SA sensors use
# different naming than the networked PLS sensors). Confirmed via testing:
# SensorDepth_Avg has no valid data before ~September 2018 -- for anything
# earlier, Tier 1 will naturally skip (too little overlapping clean data)
# and fall through to Tier 2 spline filling, no special-casing needed.
#
# Flags applied internally (before translation to upload format):
#   "raw"         -- no QC applied, original value
#   "spike"       -- detected as spike, value set to NA, eligible for filling
#   "gf_sa"       -- transmission gap filled via SA sensor relationship
#   "replaced_sa" -- spike-origin gap filled via SA sensor relationship
#   "gf_spline"   -- gap filled via spline interpolation (SA unavailable/skipped)
#   "unfilled"    -- gap too long, or no bracketing data for spline -- left NA
#
# Upload flag convention (as requested): AV = accepted value (untouched),
# "AV:EV" = accepted but estimated (spike-corrected or gap-filled), MV =
# missing value (unfillable gap). See note at the end of this file for
# concerns about this convention and an alternative worth considering.
#
# Inputs:
#   `pls_raw` -- a tibble with `measurement_time` (POSIXct) and `value`
#   (numeric) for PLS_Lvl, e.g. from sn_read_values(client, "SSN703US",
#   "5minuteSamples", "PLS_Lvl", start_date, end_date) -- no renaming
#   needed, sn_read_values() already returns measurement_time directly.
#
#   `sa_raw` -- same shape, for the SA sensor's SensorDepth_Avg, e.g. from
#   sn_read_values(client, "SA_WTS703_PT", "5minuteSamples",
#   "SensorDepth_Avg", start_date, end_date)
# =============================================================================

library(tidyverse)
library(glue)
library(zoo)   # rollmedian, na.spline


# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

# QC parameters -- these are placeholders copied from 04_stage_qc.R's stage
# thresholds. PLS_Lvl here is RAW/uncorrected data, not offset-corrected stage,
# so verify these are still sensible for this variable's actual scale before
# trusting the output -- don't assume the same threshold transfers directly.
SPIKE_RATE_PER_5MIN <- 0.03
SPIKE_WINDOW         <- 5
MAX_FILL_GAP_MINS    <- 180
SA_R2_THRESHOLD      <- 0.95

MEASUREMENT_NAME <- "PLS_Lvl"
SITE             <- "SSN703US"
QC_BY            <- "emily.haughton@hakai.org"


# -----------------------------------------------------------------------------
# 1. Load data
# -----------------------------------------------------------------------------
# Expects `pls_raw` and `sa_raw` already in scope -- swap for however you're
# actually pulling them (e.g. via sn_read_values() from sn_functions.R).

pls <- pls_raw |> arrange(measurement_time)
sa  <- sa_raw  |> arrange(measurement_time) |> select(measurement_time, stage_sa = value)

message("Loaded ", nrow(pls), " rows for ", MEASUREMENT_NAME)
message("Loaded ", nrow(sa), " rows of SA sensor data")


# -----------------------------------------------------------------------------
# 2. Spike detection
# -----------------------------------------------------------------------------
# Mirrors 04_stage_qc.R section 3: rate-of-change plus a rolling-median
# crosscheck for isolated single-point spikes.

pls <- pls |>
  mutate(
    qc_flag     = "raw",
    value_qc    = value,
    value_diff  = abs(value_qc - lag(value_qc)),
    rolling_med = rollmedian(value_qc, k = SPIKE_WINDOW, fill = NA, align = "center"),
    rolling_dev = abs(value_qc - rolling_med),
    is_spike    = qc_flag == "raw" &
      (value_diff > SPIKE_RATE_PER_5MIN | rolling_dev > 3 * SPIKE_RATE_PER_5MIN),
    qc_flag     = if_else(is_spike & !is.na(is_spike), "spike", qc_flag),
    value_qc    = if_else(qc_flag == "spike", NA_real_, value_qc)
  ) |>
  select(-value_diff, -rolling_med, -rolling_dev, -is_spike)

n_spikes <- sum(pls$qc_flag == "spike")
message("Spikes flagged: ", n_spikes, " rows")


# -----------------------------------------------------------------------------
# 3. Gap filling -- Tier 1: SA sensor relationship
# -----------------------------------------------------------------------------
# Mirrors 04_stage_qc.R section 4. Assess the linear relationship between
# PLS_Lvl and the SA sensor over clean overlapping data; if R2 >= threshold,
# use it to fill both transmission gaps and spike-origin gaps.

pls_sa <- pls |> left_join(sa, by = "measurement_time")

fit_data <- pls_sa |>
  filter(qc_flag == "raw", !is.na(value_qc), !is.na(stage_sa))

sa_relationship_usable <- nrow(fit_data) >= 100

if (!sa_relationship_usable) {
  message("Insufficient clean overlapping SA data (", nrow(fit_data),
          " rows) -- SA filling skipped, falling through to spline for all gaps")
} else {
  fit <- lm(value_qc ~ stage_sa, data = fit_data)
  r2  <- summary(fit)$r.squared
  message("SA relationship R2 = ", round(r2, 4))

  if (r2 < SA_R2_THRESHOLD) {
    message("R2 below threshold (", SA_R2_THRESHOLD, ") -- SA filling skipped")
    sa_relationship_usable <- FALSE
  }
}

if (sa_relationship_usable) {
  pls_sa <- pls_sa |>
    mutate(
      stage_sa_predicted = predict(fit, newdata = data.frame(stage_sa = stage_sa)),
      fill_flag = case_when(
        qc_flag == "spike" & !is.na(stage_sa_predicted)                    ~ "replaced_sa",
        is.na(value_qc) & qc_flag != "spike" & !is.na(stage_sa_predicted)  ~ "gf_sa",
        TRUE ~ NA_character_
      ),
      qc_flag  = if_else(!is.na(fill_flag), fill_flag, qc_flag),
      value_qc = if_else(!is.na(fill_flag), stage_sa_predicted, value_qc)
    )

  n_sa_filled <- sum(pls_sa$qc_flag %in% c("gf_sa", "replaced_sa"))
  message("Rows filled via SA relationship: ", n_sa_filled)
}

pls <- pls_sa |> select(-any_of(c("stage_sa", "stage_sa_predicted", "fill_flag")))


# -----------------------------------------------------------------------------
# 4. Gap filling -- Tier 2: spline interpolation
# -----------------------------------------------------------------------------
# Mirrors 04_stage_qc.R's Tier 2/3 spline logic (no event/baseflow split here
# for simplicity -- add EVENT_STAGE_THRESHOLD-style logic back in if that
# distinction matters for this variable too). Only applies to rows still
# unfilled after Tier 1.

pls <- pls |>
  mutate(
    is_gap = is.na(value_qc),
    # Increment on gap *starts*, not valid->valid transitions -- same fix
    # documented in 04_stage_qc.R's change log, avoids merging separate gaps.
    gap_id = cumsum(is_gap & !lag(is_gap, default = FALSE))
  )

gap_info <- pls |>
  filter(is_gap) |>
  group_by(gap_id) |>
  summarise(
    gap_mins = as.numeric(difftime(max(measurement_time), min(measurement_time), units = "mins")),
    .groups  = "drop"
  )

fillable_ids <- gap_info |> filter(gap_mins <= MAX_FILL_GAP_MINS) |> pull(gap_id)

message(nrow(gap_info |> filter(gap_mins <= MAX_FILL_GAP_MINS)),
        " gaps duration-eligible for spline, ",
        nrow(gap_info |> filter(gap_mins > MAX_FILL_GAP_MINS)),
        " too long -- left as NA")

pls <- pls |>
  left_join(gap_info, by = "gap_id") |>
  mutate(
    value_for_spline = if_else(is_gap & !(gap_id %in% fillable_ids), NA_real_, value_qc),
    value_splined    = na.spline(value_for_spline, na.rm = FALSE),
    qc_flag = case_when(
      is_gap & gap_id %in% fillable_ids ~ "gf_spline",
      is_gap                            ~ "unfilled",
      TRUE                              ~ qc_flag
    ),
    value_qc = if_else(qc_flag == "gf_spline", value_splined, value_qc)
  )

# Validation: a duration-eligible gap can still come back NA from na.spline()
# if there's no bracketing data (e.g. at the start/end of the record) -- same
# fix documented in 04_stage_qc.R. Downgrade those rather than shipping a
# "filled" flag on an empty value.
n_before_validation <- sum(pls$qc_flag == "gf_spline" & is.na(pls$value_qc))

pls <- pls |>
  mutate(
    qc_flag = if_else(qc_flag == "gf_spline" & is.na(value_qc), "unfilled", qc_flag)
  ) |>
  select(-is_gap, -gap_id, -gap_mins, -value_for_spline, -value_splined)

if (n_before_validation > 0) {
  message("WARNING -- ", n_before_validation,
          " rows flagged as spline-filled but na.spline() returned NA. Downgraded to unfilled.")
}

message("\n--- QC flag summary ---")
pls |> count(qc_flag) |> print()


# -----------------------------------------------------------------------------
# 5. Translate to the upload flag convention
# -----------------------------------------------------------------------------
# AV = accepted value (untouched raw data)
# AV:EV = accepted but estimated (spike-corrected or gap-filled, by any tier)
# MV = missing value (unfillable gap)
#
# quality_level: real QC history pulled from this station's sn_qc table
# showed quality_level = 2 for BOTH untouched raw values and heavily
# interpolated ones -- the numeric level did NOT distinguish raw from
# estimated in what we actually observed; that distinction lived entirely in
# the qc_flag text. So quality_level = 2 is used throughout here to match
# that evidence. There's no confirmed example of what an MV row's
# quality_level should be -- 4 is a guess, not evidence-based. Check with
# whoever manages ingestion validation, or look for an existing MV example
# elsewhere in the sn_qc schema, before trusting this for unfilled rows.

pls_upload <- pls |>
  mutate(
    quality_level = case_when(
      qc_flag == "unfilled" ~ 4,  # UNCONFIRMED -- see note above
      TRUE                  ~ 2
    ),
    qc_flag_code = case_when(
      qc_flag == "spike"       ~ "AV:EV: Spike removed, gap-filled via spline interpolation",
      qc_flag == "gf_sa"       ~ "AV:EV: Transmission gap filled using SA sensor relationship",
      qc_flag == "replaced_sa" ~ "AV:EV: Spike removed, gap-filled using SA sensor relationship",
      qc_flag == "gf_spline"   ~ "AV:EV: Transmission gap filled via spline interpolation",
      qc_flag == "unfilled"    ~ glue("MV: No value available (gap > {MAX_FILL_GAP_MINS} min or no bracketing data)"),
      TRUE                      ~ "AV"
    )
  )


# -----------------------------------------------------------------------------
# 6. Build the columns required for upload
# -----------------------------------------------------------------------------
# Confirmed set: measurement_time, quality_level, qc_flag, measurement_name,
# qc_by, recorded_time, PLUS avg -- the actual corrected/estimated value.
#
# This was originally missed: the six metadata columns alone match the
# working POST example seen earlier, but that turned out to be an incomplete
# read of it. Two things confirm avg is required: (1) the API docs describe
# this endpoint as posting "manual qc flags AND data" -- not flags alone, and
# (2) real historical QC rows pulled earlier from this exact station (the
# PLS_Temp "Linearly interpolated..." examples) had val = NA but avg
# populated with the actual corrected number -- avg is the working value
# column, not val.

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


# -----------------------------------------------------------------------------
# 7. Upload (NOT run automatically -- review pls_for_db first)
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
# You mentioned you're not happy with this convention -- a few concrete
# problems with it, and one option worth considering:
#
# - It collapses a lot of real diagnostic information. Your own 04_stage_qc.R
#   vocabulary (spike / gf_spline / gf_sa / replaced_sa / bad_data / unfilled /
#   ...) tells you HOW and WHY a value was touched, including which of two
#   very different fill methods (SA relationship vs spline) was used.
#   AV:EV alone doesn't distinguish those at all -- you'd be relying entirely
#   on free text staying consistent and parseable, forever, to recover that.
# - It's not self-documenting. Someone unfamiliar with hydrometric QC
#   shorthand has to already know AV/EV/MV mean Accepted/Estimated/Missing --
#   there's no way to infer that from the code itself.
# - Packing a structured code + free text into one string column (like the
#   historical "AV:EV: Linearly interpolated..." pattern) makes programmatic
#   filtering brittle -- "give me all SA-filled values" means string-matching
#   inside a sentence, not a clean boolean/categorical filter.
#
# What this script does as a compromise: keeps your own descriptive flag
# vocabulary as the actual QC record (qc_flag column, before section 5), and
# only translates to the terse AV/EV/MV-prefixed string at the point of
# upload, since that appears to be what the existing schema/convention
# expects. Nothing is lost -- the informative version, including which fill
# method was used, still exists in `pls` for your own records/analysis.
#
# If you have any say over the schema itself, the cleaner fix would be
# separate columns -- e.g. a `flag_category` (spike/gap_fill_sa/gap_fill_
# spline/bad_data/...) alongside `quality_level` and a free-text `notes`
# field -- rather than encoding everything into one string. Worth raising
# with whoever maintains the sn_qc schema if that's ever on the table.
