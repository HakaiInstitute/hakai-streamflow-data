# =============================================================================
# rating_curve_functions.R -- LOESS / power-law / extrapolation / CI helpers
# =============================================================================
# Source this ONE file. Replaces, with a clean tidyverse-native rewrite:
#   selectspan.R, interp1.R, interp2.R, HQ_unc.R, CI.R, CI_model_input.R,
#   plus the power-law / extrapolation / bootstrap-CI code that was inlined
#   (and patched three times for row misalignment) in 08_rating_curve_v5.2.Rmd
#   and re-implemented a fourth time inside recreate_MK_curve.R.
#
# Design: tibble in -> tibble out; stable, documented row order; every
# function that pairs gaugings to model values returns a KEYED tibble (on
# EventID / MID when present, else Stage_avg + Q_meas), so callers never
# rebuild a pairing from row order. `KScorrect` is no longer needed --
# rc_qmixnorm() is inline.
#
# Column conventions (match the prepped gauging table):
#   Stage_avg   -- stage in cm (the caller sets this = Stage_avg_corrected)
#   Q_meas      -- measured discharge, m3/s
#   Q_rel_unc   -- field-measured relative uncertainty, %
#   Stage_stdv  -- stage standard deviation, cm
#   Q_model / Max_CI / Min_CI -- fitted curve + CI band, m3/s
# =============================================================================

library(tidyverse)
library(splines)


# -----------------------------------------------------------------------------
# 0. Small utilities
# -----------------------------------------------------------------------------

#' Stable gauging key: EventID/MID if available, else Stage_avg + Q_meas
#'
#' @param hq A gauging tibble.
#' @return `hq` with a character `.gauge_key` column added.
#' @export
rc_add_key <- function(hq) {
  eid <- if ("EventID" %in% names(hq)) as.character(hq[["EventID"]]) else NA_character_
  mid <- if ("MID"     %in% names(hq)) as.character(hq[["MID"]])     else NA_character_
  fallback <- paste0("S", round(hq$Stage_avg, 4), "_Q", round(hq$Q_meas, 6))
  hq |>
    mutate(.gauge_key = dplyr::coalesce(
      dplyr::if_else(!is.na(eid), paste0("E", eid), NA_character_),
      dplyr::if_else(!is.na(mid), paste0("M", mid), NA_character_),
      fallback
    ))
}


#' Mixture-of-normals quantile (equal weights) -- replaces KScorrect::qmixnorm
#'
#' @param p Probability.
#' @param mean,sd Component means / sds (vectors).
#' @param expand Multiplier on component sds (KScorrect's `expand` arg).
#' @return A single quantile value.
#' @export
rc_qmixnorm <- function(p, mean, sd, expand = 1) {
  mean <- as.numeric(mean); sd <- as.numeric(sd) * expand
  ok <- is.finite(mean) & is.finite(sd) & sd > 0
  if (!any(ok)) return(NA_real_)
  mean <- mean[ok]; sd <- sd[ok]
  lo <- min(mean - 8 * sd); hi <- max(mean + 8 * sd)
  if (!is.finite(lo) || !is.finite(hi) || lo == hi) return(mean(mean))
  Fm <- function(x) mean(pnorm(x, mean, sd)) - p
  if (Fm(lo) > 0) return(lo)
  if (Fm(hi) < 0) return(hi)
  uniroot(Fm, c(lo, hi), tol = 1e-6)$root
}


# -----------------------------------------------------------------------------
# 1. LOESS
# -----------------------------------------------------------------------------

#' LOESS Q~stage at several spans, long format (was selectspan())
#'
#' @param hq A gauging tibble with `Stage_avg` and `Q_meas`.
#' @param spans Numeric vector of LOESS spans.
#' @param newdata Optional stage vector to predict on. Default: the gauged
#'   stages themselves (matches the old selectspan() behaviour).
#' @return A long tibble: `Stage_avg`, `span` (chr, e.g. `"span_0.4"`),
#'   `Q_model`.
#' @export
rc_loess_spans <- function(hq, spans, newdata = NULL) {
  x <- if (is.null(newdata)) sort(unique(hq$Stage_avg)) else sort(unique(newdata))
  map_dfr(spans, function(s) {
    m <- loess(Q_meas ~ Stage_avg, data = hq, span = s,
               control = loess.control(surface = "direct"))
    tibble(Stage_avg = x,
           span = sprintf("span_%.2f", s),
           Q_model = as.numeric(predict(m, newdata = data.frame(Stage_avg = x))))
  })
}


#' A single LOESS curve on an arbitrary stage grid
#'
#' @param hq A gauging tibble.
#' @param span LOESS span.
#' @param stage_grid Stage values to predict on.
#' @return A tibble: `Stage_avg`, `Q_model`.
#' @export
rc_loess_curve <- function(hq, span, stage_grid) {
  m <- loess(Q_meas ~ Stage_avg, data = hq, span = span,
             control = loess.control(surface = "direct"))
  tibble(Stage_avg = stage_grid,
         Q_model = as.numeric(predict(m, newdata = data.frame(Stage_avg = stage_grid))))
}


# -----------------------------------------------------------------------------
# 2. Power law  Q = a * (h - h0)^b
# -----------------------------------------------------------------------------

#' Fit a power law, with log-log starting values and a fixed-h0 fallback
#'
#' Unifies the RC2 / RC3 nls() blocks in the old Rmd and RC3_power.R. Pass
#' `h0` to fix it, or `h0_range` to grid-search it (recreate_MK_curve.R
#' style); default `h0_range` derives from the data (0.9 * min positive-Q
#' stage, matching the Rmd convention).
#'
#' @param hq A gauging tibble. Only rows with `Q_meas > 0` are used.
#' @param h0 Fixed h0 (cm), or `NULL`.
#' @param h0_range Length-2 numeric (cm) to grid-search h0 within, or `NULL`.
#' @param min_stage Only fit on gaugings at/above this stage (cm). Default
#'   `-Inf` (all).
#' @return `list(fn, a, b, h0, model)` -- `fn(h)` vectorised, `model` the
#'   fitted `nls`/`lm` object (NULL for the grid search).
#' @export
rc_fit_powerlaw <- function(hq, h0 = NULL, h0_range = NULL, min_stage = -Inf) {
  full <- hq |> filter(Q_meas > 0) |> arrange(Stage_avg)   # h0 is derived from ALL gaugings
  d    <- full |> filter(Stage_avg >= min_stage)           # ... but the fit uses only >= min_stage
  if (nrow(d) < 3) stop("rc_fit_powerlaw: fewer than 3 usable gaugings at/above min_stage = ", min_stage)

  if (is.null(h0) && is.null(h0_range)) {
    # 0.9 * the lowest positive-Q gauged stage across the WHOLE set -- not the
    # min_stage-filtered subset. (Filtering first would tie h0 to the
    # inflection point and produce a nonsense exponent.)
    h0 <- min(full$Stage_avg) * 0.9
  }

  # -- grid-search h0 (recreate_MK_curve.R behaviour) --
  if (!is.null(h0_range)) {
    hi <- min(h0_range[2], min(d$Stage_avg) - 1)
    if (h0_range[1] >= hi) {
      stop("rc_fit_powerlaw: h0_range lower bound (", h0_range[1],
           ") is at/above the max feasible h0 (min gauged stage - 1 = ",
           round(hi, 2), "). Lower h0_range[1].")
    }
    best <- NULL
    for (h0i in seq(h0_range[1], hi, by = 0.25)) {
      m <- lm(log(Q_meas) ~ log(Stage_avg - h0i), data = d)
      rss <- sum(resid(m)^2)
      if (is.null(best) || rss < best$rss) {
        best <- list(h0 = h0i, a = unname(exp(coef(m)[1])), b = unname(coef(m)[2]), rss = rss)
      }
    }
    return(list(
      fn = function(h) as.numeric(best$a * pmax(h - best$h0, 0)^best$b),
      a = best$a, b = best$b, h0 = best$h0, model = NULL
    ))
  }

  # -- nls with log-log starts, fixed-h0 fallback --
  ll   <- d |> filter(Stage_avg > h0) |> mutate(lh = log(Stage_avg - h0), lq = log(Q_meas))
  lm0  <- lm(lq ~ lh, data = ll)
  a0   <- unname(exp(coef(lm0)[1])); b0 <- unname(coef(lm0)[2])

  fit <- tryCatch(
    nls(Q_meas ~ a * (Stage_avg - h0f)^b, data = d,
        start = list(a = a0, b = b0, h0f = h0),
        control = nls.control(maxiter = 500)),
    error = function(e) {
      message("rc_fit_powerlaw: free-h0 nls failed -- fixing h0 = ", round(h0, 1))
      nls(Q_meas ~ a * (Stage_avg - h0)^b, data = d,
          start = list(a = a0, b = b0), control = nls.control(maxiter = 500))
    }
  )
  co <- coef(fit)
  h0_out <- if ("h0f" %in% names(co)) unname(co["h0f"]) else h0
  a_out  <- unname(co["a"]); b_out <- unname(co["b"])
  list(
    fn = function(h) as.numeric(a_out * pmax(h - h0_out, 0)^b_out),
    a = a_out, b = b_out, h0 = h0_out, model = fit
  )
}


# -----------------------------------------------------------------------------
# 3. Extrapolation -- anchored to the LOESS, curve continuous not co-fitted
# -----------------------------------------------------------------------------

#' Extend a LOESS curve below its lowest point with an anchored power law
#'
#' Reuses the high-end exponent `b` (no new fit -- there are no gaugings
#' down here); solves `a` analytically so the power law passes exactly
#' through the LOESS value at its own minimum. From the Rmd `rc2_extrap`
#' low-end block.
#'
#' @param loess_curve A tibble `Stage_avg`, `Q_model` (real LOESS range).
#' @param target_min Lowest stage (cm) to extend to (e.g. the sensor's true
#'   recorded minimum).
#' @param b Exponent to reuse.
#' @param h0 h0 (cm) for the extrapolated segment -- e.g. `target_min * 0.9`.
#' @param step Stage step (cm). Default 1.
#' @return A tibble `Stage_avg`, `Q_model` from `target_min` up to just
#'   below the LOESS minimum. Attributes `a`, `h0`, `anchor_stage`.
#' @export
rc_extrapolate_low <- function(loess_curve, target_min, b, h0, step = 1) {
  anchor_stage <- min(loess_curve$Stage_avg)
  anchor_q     <- loess_curve$Q_model[which.min(loess_curve$Stage_avg)]
  if (target_min >= anchor_stage) {
    return(tibble(Stage_avg = numeric(), Q_model = numeric()))
  }
  if (anchor_stage <= h0) stop("rc_extrapolate_low: LOESS minimum is at/below h0")

  a <- anchor_q / (anchor_stage - h0)^b
  stages <- seq(floor(target_min), ceiling(anchor_stage) - step, by = step)
  out <- tibble(Stage_avg = stages,
                Q_model = a * pmax(stages - h0, 1e-6)^b)
  attr(out, "a") <- a; attr(out, "h0") <- h0; attr(out, "anchor_stage") <- anchor_stage
  out
}


#' Extend a LOESS curve above its highest point with an anchored power law
#'
#' Uses a power law fitted on the upper gaugings (`fit`), rescaled to pass
#' through the LOESS value at its maximum. From the Rmd `rc2_extrap` high-end
#' block.
#'
#' @param loess_curve A tibble `Stage_avg`, `Q_model`.
#' @param fit A `rc_fit_powerlaw()` result.
#' @param cap_stage Highest stage (cm) to extend to (e.g. 218 = max recorded).
#' @param step Stage step (cm). Default 1.
#' @return A tibble `Stage_avg`, `Q_model` from just above the LOESS max up
#'   to `cap_stage`. Attribute `scale`.
#' @export
rc_extrapolate_high <- function(loess_curve, fit, cap_stage, step = 1) {
  anchor_stage <- max(loess_curve$Stage_avg)
  if (anchor_stage + step > cap_stage) {
    # LOESS already reaches (or passes) the cap -- nothing to extrapolate.
    return(tibble(Stage_avg = numeric(), Q_model = numeric()))
  }
  anchor_q <- loess_curve$Q_model[which.max(loess_curve$Stage_avg)]
  scale <- anchor_q / fit$fn(anchor_stage)
  stages <- seq(anchor_stage + step, cap_stage, by = step)
  out <- tibble(Stage_avg = stages, Q_model = fit$fn(stages) * scale)
  attr(out, "scale") <- scale
  out
}


# -----------------------------------------------------------------------------
# 4. mm interpolation (was interp1 / interp2)
# -----------------------------------------------------------------------------

#' Interpolate a curve onto a fixed stage grid
#'
#' @param curve A tibble with `Stage_avg`, `Q_model`, and optionally
#'   `Max_CI` / `Min_CI`.
#' @param step Stage step (cm). Default 0.1.
#' @return A tibble on the `step` grid, same columns as the input (CI
#'   columns interpolated too when present). Rounded: stage 1 dp, Q/CI 4 dp.
#' @export
rc_interp_grid <- function(curve, step = 0.1) {
  curve <- curve |> distinct(Stage_avg, .keep_all = TRUE) |> arrange(Stage_avg)

  n_neg <- sum(curve$Q_model < 0, na.rm = TRUE)
  if (n_neg > 0) {
    warning("rc_interp_grid: ", n_neg, " curve point(s) had Q_model < 0 (likely a ",
            "LOESS undershoot at the low end or a bad low-flow extrapolation) -- ",
            "clamped to 0. Check the fit.")
  }

  lo <- round(min(curve$Stage_avg), 1); hi <- round(max(curve$Stage_avg), 1)
  grid <- round(seq(lo, hi, by = step), 1)   # integer-count grid, no fp drift

  out <- tibble(
    Stage_avg = grid,
    Q_model   = round(pmax(approx(curve$Stage_avg, curve$Q_model, xout = grid, rule = 2)$y, 0), 4)
  )
  for (col in intersect(c("Max_CI", "Min_CI"), names(curve))) {
    ok <- !is.na(curve[[col]])
    if (sum(ok) < 2) { out[[col]] <- NA_real_; next }
    out[[col]] <- round(pmax(approx(curve$Stage_avg[ok], curve[[col]][ok],
                                    xout = grid, rule = 2)$y, 0), 4)
  }
  out
}


# -----------------------------------------------------------------------------
# 5. Stage-uncertainty propagation (was HQ_unc) -- keyed output
# -----------------------------------------------------------------------------

#' Propagate stage sd into discharge uncertainty
#'
#' Adds the stage-measurement component (how much Q moves if stage moves by
#' `Stage_stdv`) in quadrature with the field-measured `Q_rel_unc`.
#'
#' @param hq A gauging tibble with `Stage_avg`, `Stage_stdv`, `Q_meas`,
#'   `Q_rel_unc` (and ideally `EventID`/`MID` for the key).
#' @param curve_fn A function `Q_model(stage_cm)` -- e.g. the interpolated
#'   full curve as `approxfun(...)`, or `fit$fn`.
#' @return A tibble keyed on `.gauge_key`, plus `Stage_avg`, `Q_meas`,
#'   `Q_H_rel_unc`, `Q_H_abs_unc`, `Q_max`, `Q_min`. Row order = input order.
#' @export
rc_stage_uncertainty <- function(hq, curve_fn) {
  hq <- rc_add_key(hq)
  n_na_stdv <- sum(is.na(hq$Stage_stdv))
  n_na_qru  <- sum(is.na(hq$Q_rel_unc))
  if (n_na_stdv > 0 || n_na_qru > 0) {
    warning("rc_stage_uncertainty: ", n_na_stdv, " gauging(s) missing Stage_stdv, ",
            n_na_qru, " missing Q_rel_unc -- treating the missing component as 0.")
  }
  hq |>
    transmute(
      .gauge_key, Stage_avg,
      Stage_stdv = dplyr::coalesce(Stage_stdv, 0),
      Q_meas,
      Q_rel_unc  = dplyr::coalesce(Q_rel_unc, 0),
      Q_model          = curve_fn(Stage_avg),
      Q_model_add_stdv = curve_fn(Stage_avg + Stage_stdv),
      rel_unc_stage    = dplyr::if_else(Q_model > 0,
                                        pmax((Q_model_add_stdv - Q_model) / Q_model * 100, 0),
                                        0),
      Q_H_rel_unc      = sqrt(Q_rel_unc^2 + rel_unc_stage^2),
      Q_H_abs_unc      = Q_H_rel_unc / 100 * Q_meas,
      Q_max            = Q_meas + Q_H_abs_unc,
      # left unclamped (can be < 0 for sparse low-flow gaugings) to match the
      # established method -- rc_bootstrap_ci() clamps the final band at 0.
      Q_min            = Q_meas - Q_H_abs_unc
    )
}


# -----------------------------------------------------------------------------
# 6. Bootstrap CI (was CI + CI_model_input) -- keyed to the stage grid
# -----------------------------------------------------------------------------

#' Bootstrap mixture-normal CI band around a LOESS curve
#'
#' For each of `n_boot` iterations: resample each gauging's discharge from
#' {Q_meas, Q_min, Q_max}, refit the LOESS at `span`, predict fit + se on
#' `stage_grid`. The CI at each grid stage is the p / 1-p mixture-normal
#' quantile across the `n_boot` (fit, se) pairs.
#'
#' @param hq_unc A tibble from [rc_stage_uncertainty()] (needs `Stage_avg`,
#'   `Q_meas`, `Q_min`, `Q_max`).
#' @param stage_grid Stage values (cm) to return the band on.
#' @param span LOESS span (same as the median curve).
#' @param n_boot Iterations. Default 500 (MK's value).
#' @param p Upper tail probability. Default 0.95 (=> 90% band).
#' @param expand `rc_qmixnorm()` expand arg. Default 1.
#' @return A tibble: `Stage_avg`, `Max_CI`, `Min_CI` (keyed to `stage_grid`).
#' @export
rc_bootstrap_ci <- function(hq_unc, stage_grid, span, n_boot = 500, p = 0.95, expand = 1) {
  stage_grid <- sort(unique(stage_grid))
  draw_pool  <- pmap(list(hq_unc$Q_meas, hq_unc$Q_min, hq_unc$Q_max),
                     ~ c(..1, ..2, ..3))

  fitmat <- matrix(NA_real_, length(stage_grid), n_boot)
  semat  <- matrix(NA_real_, length(stage_grid), n_boot)

  for (b in seq_len(n_boot)) {
    q_b <- map_dbl(draw_pool, ~ sample(.x, 1))
    dfb <- tibble(Stage_avg = hq_unc$Stage_avg, Q = q_b)
    pr <- predict(
      loess(Q ~ Stage_avg, data = dfb, span = span,
            control = loess.control(surface = "direct")),
      newdata = data.frame(Stage_avg = stage_grid), se = TRUE
    )
    fitmat[, b] <- pr$fit
    semat[, b]  <- pr$se.fit
  }

  tibble(
    Stage_avg = stage_grid,
    Max_CI = map_dbl(seq_along(stage_grid), \(j) rc_qmixnorm(p,     fitmat[j, ], semat[j, ], expand)),
    Min_CI = map_dbl(seq_along(stage_grid), \(j) rc_qmixnorm(1 - p, fitmat[j, ], semat[j, ], expand))
  ) |>
    mutate(Max_CI = pmax(Max_CI, 0), Min_CI = pmax(Min_CI, 0))
}


# -----------------------------------------------------------------------------
# 7. Extrapolated CI -- anchor-and-widen with an absolute floor
# -----------------------------------------------------------------------------

#' CI band for an extrapolated curve segment
#'
#' No new fitting. The band's relative half-width starts at
#' `anchor_ci_pct` (the real CI % at the join point) and grows toward `h0`
#' as `(fraction_to_h0)^growth_power`, capped at `mult_cap` x the anchor.
#' An absolute floor (`floor_abs`, also grown by the same curve) stops a
#' percentage band from collapsing to zero width as Q_model -> 0 near h0.
#' From the Rmd `rc2_ci` / `low_flow_unc_apply` blocks.
#'
#' @param seg A tibble `Stage_avg`, `Q_model` (the extrapolated segment).
#' @param anchor_stage Stage (cm) of the join to the real curve.
#' @param h0 h0 (cm) -- the far end of the unsupported range.
#' @param anchor_ci_pct Relative half-width at the anchor (fraction, e.g.
#'   0.1 for +/-10%).
#' @param floor_abs Absolute half-width floor at the anchor (m3/s).
#' @param mult_cap CI multiplier at h0. Default 3.
#' @param growth_power >1 accelerates widening toward h0. Default 2.
#' @return `seg` with `Max_CI`, `Min_CI` added.
#' @export
rc_extrapolated_ci <- function(seg, anchor_stage, h0, anchor_ci_pct, floor_abs,
                                mult_cap = 3, growth_power = 2) {
  full_range <- abs(anchor_stage - h0)
  seg |>
    mutate(
      frac  = pmin(pmax(abs(anchor_stage - Stage_avg) / full_range, 0), 1),
      mult  = 1 + (mult_cap - 1) * frac^growth_power,
      hw    = pmax(Q_model * anchor_ci_pct * mult, floor_abs * mult),
      Max_CI = Q_model + hw,
      Min_CI = pmax(Q_model - hw, 0)
    ) |>
    select(-frac, -mult, -hw)
}


# -----------------------------------------------------------------------------
# 8. Pooled low-flow uncertainty trend
# -----------------------------------------------------------------------------

#' Fit a discharge-dependent relative-uncertainty trend (RC2 + RC3 pooled)
#'
#' `log(Q_rel_unc) ~ log(Q_meas)` on pooled gaugings: relative uncertainty
#' rises as discharge falls. From the Rmd "Low-flow uncertainty model".
#'
#' @param pool A tibble with `Q_meas` and `Q_rel_unc` (> 0).
#' @param max_pct Ceiling on the projected relative uncertainty (%).
#'   Default 150.
#' @return `list(predict_pct, r2, model)` -- `predict_pct(Q)` returns the
#'   projected relative uncertainty in %.
#' @export
rc_low_flow_unc_model <- function(pool, max_pct = 150) {
  pool <- pool |> filter(!is.na(Q_rel_unc), Q_rel_unc > 0, Q_meas > 0)
  if (nrow(pool) < 15) {
    warning("rc_low_flow_unc_model: < 15 gaugings -- treat the trend as a rough first pass.")
  }
  fit <- lm(log(Q_rel_unc) ~ log(Q_meas), data = pool)
  list(
    predict_pct = function(Q) pmin(exp(predict(fit, newdata = data.frame(Q_meas = Q))), max_pct),
    r2 = summary(fit)$r.squared,
    model = fit
  )
}
