# =============================================================================
# recreate_mk_curve.R -- recreate MK's original SSN703 rating curve (v4, R1-3)
# =============================================================================
# Sources rating_curve_functions.R (for rc_qmixnorm() only -- the rest of MK's
# method is bespoke and stays inline). Needs: tidyverse, plotly. KScorrect is
# NOT required.
#
# This is the regression guard for the datum-offset direction: Part B is the
# check that proved ssn703_a is MK's reference datum (median MK-a ~ 0 for
# Ratings 1-2; MK-b ~ -1.9 cm for the Rating-3 tail). If prep_gaugings.R or
# offsets.csv ever drift back to the old "+2 cm onto ssn703_b" convention,
# Part A's match to the published curve degrades and Part B's deltas move.
# See memory/mk-rc1-stage-provenance.md and 03_docs/metadata/offsets.csv.
#
# WHAT THIS DOES
#   Part A - rebuilds Ratings 1, 2, 3 from MK's own gauging table using her
#            method (LOESS span-select -> power-law tails -> mm interpolation
#            -> stage-uncertainty propagation -> bootstrap mixture-normal CI),
#            then overlays the result on her published lookup
#            (ssn703_RC1_lookup_v_previous.csv) and prints goodness-of-match.
#
#   Part B - independent stage-provenance check: at every MK gauging time it
#            pulls raw ssn703_a / _b / _c stage (cm) and compares to the
#            Stage_avg MK recorded, per rating period, so you can SEE which
#            sensor she tracked and what adjustment she applied.
#
# TWO THINGS TO KNOW GOING IN (both matter for interpretation)
#   1. Emily's note: MK built her curves on the ADJUSTED WtrLvl / sensor-a
#      channel, not ssn703_b. Her gauging-table Stage_avg already carries that
#      adjusted stage, so Part A fits it as-is and does not care which sensor
#      it came from. Part B is where the sensor question gets answered.
#   2. Raw ssn703_a (WtrLvlSSN703US) stops at 2018-09-14 18:00 - it is empty
#      for every gauging after that. MK's Rating 3 set has ~22 gaugings from
#      2018-09 to 2019-02 that therefore CANNOT be raw sensor a. The repo's
#      07a_recover_historical_stage_rc1.R concluded she used ssn703_b for that
#      window. Part B will show what her Stage_avg actually matches.
#
# INPUTS
#   03_docs/metadata/MK_original_gauging_table_703.csv
#   03_docs/metadata/ssn703_RC1_lookup_v_previous.csv
#   01_raw/SSN703/ssn703_a.csv, ssn703_b.csv, ssn703_c.csv   (Part B only)
#
# OUTPUTS  (04_outputs/curve_validation/)
#   MK_recreated_lookup.csv
#   MK_recreated_vs_published.html
#   MK_stage_provenance.html
#   + console diagnostics
#
# Run:  Rscript 02_processing/scripts/rating_curve/recreate_mk_curve.R
#       (from the project root)
# =============================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(plotly)
  library(htmlwidgets)
})

source("02_processing/scripts/rating_curve/rating_curve_functions.R")

set.seed(703)

# -----------------------------------------------------------------------------
# 0. CONFIG  -- tune here to converge on MK's published curve
# -----------------------------------------------------------------------------

proj_root <- getwd()                       # expects project root as cwd
meta_dir  <- file.path(proj_root, "03_docs/metadata")
raw_dir   <- file.path(proj_root, "01_raw/SSN703")
out_dir   <- file.path(proj_root, "04_outputs/curve_validation")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

TZ <- "Etc/GMT+8"                          # PST, no DST (matches 01_load_stage_data.R)

# Rating-period breakpoints -- taken straight from the Start column of
# ssn703_RC1_lookup_v_previous.csv (2012-01-01 / 2017-10-12 / 2018-05-05)
RATING_BREAKS <- tibble(
  rating = c(1L, 2L, 3L),
  start  = ymd(c("2012-01-01", "2017-10-12", "2018-05-05"), tz = TZ),
  end    = ymd(c("2017-10-12", "2018-05-05", "2100-01-01"), tz = TZ)
)

# Stage grid for the final lookup (matches her lookup: 29.6 -> 218 by 0.1 cm)
STAGE_MIN  <- 29.6
STAGE_MAX  <- 218
STAGE_STEP <- 0.1

# -- MK's DOCUMENTED method (from her fitting notes) --------------------------
#   "The power law equation was fitted on the measurements of Shift 1 above an
#    inflection point of 140 cm. It was assumed that the data of Shift 2 and
#    Shift 3 (which lack high flow measurements) follow the same power law shape
#    as Shift 1, but offset to fit their data."
#
#   Rating 1  : LOESS over the gauged range, then Shift-1 power law above
#               INFLECTION (fitted only on Shift-1 gaugings >= INFLECTION).
#   Rating 2/3: their own LOESS over their (lower) gauged range; the high-flow
#               extrapolation is Shift-1's power-law SHAPE plus a constant
#               discharge offset c_s fitted to that shift's gaugings. Below
#               their gauged range the curve follows Rating 1.
#   The published lookup confirms the offset is additive in Q and flat at high
#   stage: R2 - R1 -> +3.40 m3/s and R3 - R1 -> +1.50 m3/s above ~190 cm.

INFLECTION     <- 140            # cm; LOESS -> power-law handover for Rating 1
PL_FIT_MIN     <- 100            # cm; Shift-1 gaugings >= this are used to FIT the power law
                                #     (the >=140 points alone are too few to constrain it;
                                #      matching MK's published tail needs the upper ~third)
H0_RANGE       <- c(15, 36)      # cm; plausible range for power-law h0 (curve bottoms ~29.6)
SPAN           <- c(`1` = 0.50, `2` = 0.50, `3` = 0.55)   # LOESS span per rating
OFFSET_FIT_MIN <- 100            # cm; fit Shift 2/3 Q-offset on their gaugings >= this
BLEND_CM       <- 20             # cm window to blend LOESS end -> offset power law

DO_BOOTSTRAP_CI <- TRUE      # FALSE = skip CI (fast) while tuning the median curve
N_BOOT          <- 500       # MK used 500
CI_EXPAND       <- 1         # KScorrect qmixnorm `expand` arg; the Rmd uses 1

# -----------------------------------------------------------------------------
# 1. MK's bespoke helper functions (her method -- kept inline)
#    Shared machinery (mixture-normal quantile) comes from
#    rating_curve_functions.R: rc_qmixnorm().
# -----------------------------------------------------------------------------
qmixnorm_ <- rc_qmixnorm

# LOESS Q~H at one span, predicted on an arbitrary stage vector
loess_predict <- function(df, span, newstage) {
  m <- loess(Q_meas ~ Stage_avg, data = df, span = span,
             control = loess.control(surface = "direct"))
  as.numeric(predict(m, newdata = data.frame(Stage_avg = newstage)))
}

# power-law Q = a*(h - h0)^b. h0 grid-searched within h0_range, a,b from a
# log-log linear model at each h0. Returns list(fn, a, b, h0).
fit_powerlaw <- function(df, h0_range) {
  d  <- df |> filter(Q_meas > 0) |> arrange(Stage_avg)
  hi <- min(h0_range[2], min(d$Stage_avg) - 1)
  best <- NULL
  for (h0 in seq(h0_range[1], hi, by = 0.25)) {
    m   <- lm(log(Q_meas) ~ log(Stage_avg - h0), data = d)
    rss <- sum(resid(m)^2)
    if (is.null(best) || rss < best$rss)
      best <- list(h0 = h0, a = unname(exp(coef(m)[1])), b = unname(coef(m)[2]),
                   rss = rss)
  }
  with(best, list(fn = function(h) as.numeric(a * pmax(h - h0, 0)^b),
                  a = a, b = b, h0 = h0))
}

# HQ_unc: propagate stage sd into Q uncertainty exactly as MK's HQ_unc.R
hq_unc <- function(HQ, curve_grid) {
  approx_Q <- function(x) approx(curve_grid$Stage_avg, curve_grid$Q_model,
                                 xout = x, rule = 1)$y
  HQ |>
    transmute(
      Stage_avg, Stage_stdv, Q_meas, Q_rel_unc,
      Q_model          = approx_Q(Stage_avg),
      Q_model_add_stdv = approx_Q(Stage_avg + Stage_stdv),
      Rel_unc_stage    = pmax((Q_model_add_stdv - Q_model) / Q_model * 100, 0),
      Q_H_rel_unc      = sqrt(Q_rel_unc^2 + Rel_unc_stage^2),
      Q_H_abs_unc      = Q_H_rel_unc / 100 * Q_meas,
      Q_max            = Q_meas + Q_H_abs_unc,
      Q_min            = Q_meas - Q_H_abs_unc
    ) |>
    arrange(Stage_avg)
}

# -----------------------------------------------------------------------------
# 2. LOAD MK's gauging table
# -----------------------------------------------------------------------------

mk <- read_csv(file.path(meta_dir, "MK_original_gauging_table_703.csv"),
               col_types = cols(Date = col_character(),
                                Start_time = col_character(),
                                .default = col_guess())) |>
  mutate(datetime = ymd_hm(paste(Date, Start_time), tz = TZ)) |>
  filter(!is.na(datetime), !is.na(Stage_avg), !is.na(Q_meas))

# assign rating period by date, and cross-check against MK's own Shift decimal
mk <- mk |>
  mutate(rating = case_when(
    datetime <  RATING_BREAKS$start[2] ~ 1L,
    datetime <  RATING_BREAKS$start[3] ~ 2L,
    TRUE                               ~ 3L
  ),
  shift_decimal = suppressWarnings(as.integer(round((Shift %% 1) * 10))))

message("--- MK gaugings by rating (date-assigned) vs her Shift decimal ---")
mk |> count(rating, shift_decimal, Final_rating_curve) |> print(n = 50)

fit_set <- mk |> filter(Final_rating_curve == "Y")
message("\nFitting set (Final_rating_curve == 'Y'): ", nrow(fit_set), " gaugings")
fit_set |> group_by(rating) |>
  summarise(n = n(), h_min = min(Stage_avg), h_max = max(Stage_avg),
            .groups = "drop") |> print()

# -----------------------------------------------------------------------------
# 3. PUBLISHED lookup for comparison
# -----------------------------------------------------------------------------

pub <- read_csv(file.path(meta_dir, "ssn703_RC1_lookup_v_previous.csv"),
                show_col_types = FALSE) |>
  transmute(rating = as.integer(Rating), Stage_avg, Q_pub = Q_model,
            MaxCI_pub = Max_CI, MinCI_pub = Min_CI)

# -----------------------------------------------------------------------------
# 4. BUILD the ratings with MK's documented method
# -----------------------------------------------------------------------------

stage_grid <- seq(STAGE_MIN, STAGE_MAX, by = STAGE_STEP)

loess_at <- function(HQ, span, hgrid)
  as.numeric(predict(
    loess(Q_meas ~ Stage_avg, data = HQ, span = span,
          control = loess.control(surface = "direct")),
    newdata = data.frame(Stage_avg = hgrid)))

# add MK's bootstrap mixture-normal CI band to a finished median curve
add_ci <- function(curve, HQ, span) {
  if (!DO_BOOTSTRAP_CI)
    return(mutate(curve, Max_CI = NA_real_, Min_CI = NA_real_))
  HQu   <- hq_unc(HQ, curve)
  draws <- map(seq_len(nrow(HQu)), \(i)
    sample(c(HQu$Q_meas[i], HQu$Q_min[i], HQu$Q_max[i]), N_BOOT, replace = TRUE))
  fitmat <- matrix(NA_real_, nrow(curve), N_BOOT)
  semat  <- matrix(NA_real_, nrow(curve), N_BOOT)
  for (b in seq_len(N_BOOT)) {
    dfb <- tibble(Stage_avg = HQu$Stage_avg, Q = map_dbl(draws, b))
    pr  <- predict(loess(Q ~ Stage_avg, data = dfb, span = span,
                         control = loess.control(surface = "direct")),
                   newdata = data.frame(Stage_avg = curve$Stage_avg), se = TRUE)
    fitmat[, b] <- pr$fit; semat[, b] <- pr$se.fit
  }
  curve |> mutate(
    Max_CI = map_dbl(seq_len(n()), \(j) qmixnorm_(0.95, fitmat[j, ], semat[j, ], CI_EXPAND)),
    Min_CI = map_dbl(seq_len(n()), \(j) qmixnorm_(0.05, fitmat[j, ], semat[j, ], CI_EXPAND)),
    Max_CI = pmax(Max_CI, Q_model, 0),
    Min_CI = pmax(pmin(Min_CI, Q_model), 0)
  )
}

# ---- Rating 1: LOESS (gauged range) + Shift-1 power law above INFLECTION ----
HQ1 <- fit_set |> filter(rating == 1) |> arrange(Stage_avg)
g1  <- range(HQ1$Stage_avg)
pl1 <- fit_powerlaw(HQ1 |> filter(Stage_avg >= PL_FIT_MIN), H0_RANGE)

mid1_h  <- stage_grid[stage_grid <= INFLECTION]
mid1_q  <- loess_at(HQ1, SPAN["1"], mid1_h)
up1_h   <- stage_grid[stage_grid > INFLECTION]
anchor1 <- tail(mid1_q, 1) - pl1$fn(INFLECTION)     # make the join continuous
up1_q   <- pl1$fn(up1_h) + anchor1

curve1 <- tibble(Stage_avg = c(mid1_h, up1_h),
                 Q_model   = pmax(c(mid1_q, up1_q), 0)) |> arrange(Stage_avg)
# full Rating-1 curve (LOESS + anchored power law) as a lookup function
curve1_fn <- function(h) approx(curve1$Stage_avg, curve1$Q_model, xout = h, rule = 2)$y
curve1 <- add_ci(curve1, HQ1, SPAN["1"])

message(sprintf(
  "\nRating 1: LOESS to %g cm, then power law fitted on Shift-1 gaugings >= %g cm:",
  INFLECTION, PL_FIT_MIN))
message(sprintf(
  "          a=%.3g  b=%.3f  h0=%.1f cm   (join anchor %+.3f m3/s at h=%g)",
  pl1$a, pl1$b, pl1$h0, anchor1, INFLECTION))

# ---- Ratings 2 & 3: own LOESS + Rating-1 curve shape + constant Q offset -----
# MK: Shift 2/3 lack high-flow data, so their high-flow limb = Rating 1's curve
# shifted by a constant discharge offset c_s fitted to their upper gaugings.
build_shift <- function(r) {
  HQ <- fit_set |> filter(rating == r) |> arrange(Stage_avg)
  g  <- range(HQ$Stage_avg)
  sp <- SPAN[as.character(r)]

  # c_s from this shift's UPPER gaugings (top of its range, where the shared
  # power-law shape applies): median( Q_meas - Rating1_curve(h) )
  hi  <- HQ |> filter(Stage_avg >= max(OFFSET_FIT_MIN, quantile(Stage_avg, 0.5)),
                      Q_meas > 0)
  c_s <- median(hi$Q_meas - curve1_fn(hi$Stage_avg))

  below_h <- stage_grid[stage_grid < g[1]]
  below_q <- curve1_fn(below_h)                       # low flow follows Rating 1

  mid_h <- stage_grid[stage_grid >= g[1] & stage_grid <= g[2]]
  mid_q <- loess_at(HQ, sp, mid_h)

  up_h     <- stage_grid[stage_grid > g[2]]
  up_target <- curve1_fn(up_h) + c_s                  # Rating 1 shape + offset
  join_off <- tail(mid_q, 1) - (curve1_fn(g[2]) + c_s)   # LOESS end vs target
  taper    <- pmax(0, 1 - (up_h - g[2]) / BLEND_CM)
  up_q     <- up_target + join_off * taper

  curve <- tibble(Stage_avg = c(below_h, mid_h, up_h),
                  Q_model   = pmax(c(below_q, mid_q, up_q), 0)) |> arrange(Stage_avg)
  curve <- add_ci(curve, HQ, sp)

  message(sprintf("Rating %d: LOESS %.0f-%.0f cm | fitted Q offset c_%d = %+.2f m3/s%s",
                  r, g[1], g[2], r, c_s,
                  if (r == 2) "  (published high-stage: +3.40)"
                  else        "  (published high-stage: +1.50)"))
  list(rating = r, curve = curve, c_s = c_s, span = sp, gauged = g, gaugings = HQ)
}

built <- list(
  `1` = list(rating = 1L, curve = curve1, c_s = NA_real_, span = SPAN["1"],
             gauged = g1, gaugings = HQ1),
  `2` = build_shift(2L),
  `3` = build_shift(3L)
)

recreated <- map_dfr(built, \(x) mutate(x$curve, rating = x$rating))
gaug_all  <- map_dfr(built, \(x) mutate(
  x$gaugings |> select(Stage_avg, Q_meas, Stage_stdv, Q_rel_unc, Event_no,
                       datetime, Method),
  rating = x$rating))

# -----------------------------------------------------------------------------
# 5. COMPARE recreated vs published
# -----------------------------------------------------------------------------

cmp <- recreated |>
  inner_join(pub, by = c("rating", "Stage_avg")) |>
  mutate(resid = Q_model - Q_pub,
         pct   = 100 * resid / pmax(Q_pub, 1e-6))

message("\n=====================================================================")
message(" RECREATED vs PUBLISHED  (Q_model difference, m3/s and %)")
message("=====================================================================")
cmp |>
  group_by(rating) |>
  summarise(
    n           = n(),
    rmse        = sqrt(mean(resid^2)),
    median_abs  = median(abs(resid)),
    p95_abs     = quantile(abs(resid), .95),
    max_abs     = max(abs(resid)),
    median_pct  = median(abs(pct)),
    .groups = "drop"
  ) |> print()

message("\nBy stage band (all ratings):")
cmp |>
  mutate(band = cut(Stage_avg, c(0, 50, 80, 120, 160, 999),
                    labels = c("<50", "50-80", "80-120", "120-160", ">160"))) |>
  group_by(rating, band) |>
  summarise(median_abs = median(abs(resid)),
            median_pct = median(abs(pct)), .groups = "drop") |>
  pivot_wider(names_from = band, values_from = c(median_abs, median_pct)) |>
  print(width = Inf)

write_csv(
  recreated |>
    mutate(Start = format(RATING_BREAKS$start[rating], "%m/%d/%Y %H:%M")) |>
    select(Start, Stage_avg, Q_model, Max_CI, Min_CI, Rating = rating),
  file.path(out_dir, "MK_recreated_lookup.csv")
)
message("\nSaved: ", file.path(out_dir, "MK_recreated_lookup.csv"))

# -----------------------------------------------------------------------------
# 6. PLOT recreated vs published (interactive, one panel per rating)
# -----------------------------------------------------------------------------

mk_plot <- function(r) {
  cv <- recreated |> filter(rating == r)
  pb <- pub       |> filter(rating == r)
  gg <- gaug_all  |> filter(rating == r)
  p <- plot_ly()
  if (DO_BOOTSTRAP_CI && any(is.finite(cv$Max_CI)))
    p <- add_ribbons(p, data = cv, x = ~Stage_avg, ymin = ~Min_CI, ymax = ~Max_CI,
                     line = list(width = 0), fillcolor = "rgba(150,150,150,0.25)",
                     name = "recreated 90% CI")
  p |>
    add_lines(data = pb, x = ~Stage_avg, y = ~Q_pub,
              line = list(color = "black", dash = "dash"),
              name = "MK published") |>
    add_lines(data = cv, x = ~Stage_avg, y = ~Q_model,
              line = list(color = "#E41A1C"), name = "recreated") |>
    add_markers(data = gg, x = ~Stage_avg, y = ~Q_meas,
                marker = list(color = "#377EB8", size = 6),
                text = ~paste0("Event ", Event_no, " | ", as.Date(datetime),
                               " | ", Method),
                name = "MK gaugings") |>
    layout(title = paste0("SSN703 Rating ", r,
                          "  (recreated vs MK published)"),
           xaxis = list(title = "Stage (cm)"),
           yaxis = list(title = "Discharge (m3/s)"))
}

overlay <- subplot(map(c(1, 2, 3), mk_plot), nrows = 3, shareX = TRUE,
                   titleY = TRUE, margin = 0.05)
saveWidget(overlay, file.path(out_dir, "MK_recreated_vs_published.html"),
           selfcontained = TRUE)
message("Saved: ", file.path(out_dir, "MK_recreated_vs_published.html"))

# =============================================================================
# PART B  --  STAGE PROVENANCE:  what does MK's Stage_avg actually match?
# =============================================================================

read_raw <- function(letter) {
  f <- file.path(raw_dir, paste0("ssn703_", letter, ".csv"))
  read_csv(f, skip = 4,
           col_names = c("timestamp", "year", "month", "water_year",
                         "stage_inst", "stage_avg", "stage_min",
                         "stage_max", "stage_sd"),
           col_types = cols(timestamp = col_character(), .default = col_guess()),
           na = c("", "NA", "NaN")) |>
    transmute(timestamp = ymd_hms(timestamp, tz = TZ),
              !!paste0("stage_", letter) := stage_avg * 100)  # m -> cm
}

message("\n--- Part B: reading raw sensor series (a, b, c) ---")
raw_wide <- reduce(list(read_raw("a"), read_raw("b"), read_raw("c")),
                   full_join, by = "timestamp") |>
  arrange(timestamp)

# nearest raw row within 10 min of each gauging
nearest_raw <- function(t) {
  i <- which.min(abs(raw_wide$timestamp - t))
  if (length(i) == 0) return(tibble(dt_diff_min = NA))
  d <- as.numeric(abs(raw_wide$timestamp[i] - t), units = "mins")
  raw_wide[i, ] |> mutate(dt_diff_min = d)
}

prov <- mk |>
  filter(Final_rating_curve == "Y") |>
  select(Event_no, datetime, rating, Method, Stage_avg_MK = Stage_avg) |>
  rowwise() |>
  mutate(nr = list(nearest_raw(datetime))) |>
  unnest(nr) |>
  ungroup() |>
  mutate(
    d_a = Stage_avg_MK - stage_a,
    d_b = Stage_avg_MK - stage_b,
    d_c = Stage_avg_MK - stage_c
  )

message("\n MK Stage_avg  minus  raw sensor stage  (cm), median by rating period")
message(" (small + stable delta = that's the sensor she used, delta = her adjustment)")
prov |>
  group_by(rating) |>
  summarise(
    n            = n(),
    `med d(MK-a)` = round(median(d_a, na.rm = TRUE), 2),
    `n a`        = sum(!is.na(d_a)),
    `med d(MK-b)` = round(median(d_b, na.rm = TRUE), 2),
    `n b`        = sum(!is.na(d_b)),
    `med d(MK-c)` = round(median(d_c, na.rm = TRUE), 2),
    `n c`        = sum(!is.na(d_c)),
    .groups = "drop"
  ) |> print(width = Inf)

message("\n Same, split at the 2018-09-14 sensor-a cutoff:")
prov |>
  mutate(era = if_else(datetime < ymd("2018-09-14", tz = TZ),
                       "pre 2018-09-14", "post 2018-09-14")) |>
  group_by(era) |>
  summarise(
    n            = n(),
    `med d(MK-a)` = round(median(d_a, na.rm = TRUE), 2), `n a` = sum(!is.na(d_a)),
    `med d(MK-b)` = round(median(d_b, na.rm = TRUE), 2), `n b` = sum(!is.na(d_b)),
    `med d(MK-c)` = round(median(d_c, na.rm = TRUE), 2), `n c` = sum(!is.na(d_c)),
    .groups = "drop"
  ) |> print(width = Inf)

write_csv(prov |> select(Event_no, datetime, rating, Method, Stage_avg_MK,
                         stage_a, stage_b, stage_c, d_a, d_b, d_c, dt_diff_min),
          file.path(out_dir, "MK_stage_provenance.csv"))

prov_long <- prov |>
  select(datetime, rating, d_a, d_b, d_c) |>
  pivot_longer(c(d_a, d_b, d_c), names_to = "sensor", values_to = "delta_cm") |>
  filter(!is.na(delta_cm)) |>
  mutate(sensor = recode(sensor, d_a = "MK - ssn703_a",
                         d_b = "MK - ssn703_b", d_c = "MK - ssn703_c"))

pB <- plot_ly(prov_long, x = ~datetime, y = ~delta_cm, color = ~sensor,
              type = "scatter", mode = "markers",
              marker = list(size = 7)) |>
  layout(title = "MK Stage_avg minus raw sensor stage, over time",
         xaxis = list(title = NULL),
         yaxis = list(title = "MK - raw  (cm)"),
         shapes = list(
           list(type = "line", x0 = as.numeric(ymd("2017-10-12", tz = TZ)) * 1000,
                x1 = as.numeric(ymd("2017-10-12", tz = TZ)) * 1000,
                y0 = -30, y1 = 30, line = list(dash = "dot", color = "grey")),
           list(type = "line", x0 = as.numeric(ymd("2018-05-05", tz = TZ)) * 1000,
                x1 = as.numeric(ymd("2018-05-05", tz = TZ)) * 1000,
                y0 = -30, y1 = 30, line = list(dash = "dot", color = "grey")),
           list(type = "line", x0 = as.numeric(ymd("2018-09-14", tz = TZ)) * 1000,
                x1 = as.numeric(ymd("2018-09-14", tz = TZ)) * 1000,
                y0 = -30, y1 = 30, line = list(dash = "dash", color = "orange"))
         ))
saveWidget(pB, file.path(out_dir, "MK_stage_provenance.html"), selfcontained = TRUE)
message("Saved: ", file.path(out_dir, "MK_stage_provenance.html"))
message("Saved: ", file.path(out_dir, "MK_stage_provenance.csv"))

message("\nDone.")
message("Read the console tables first, then open:")
message("  ", file.path(out_dir, "MK_recreated_vs_published.html"))
message("  ", file.path(out_dir, "MK_stage_provenance.html"))
