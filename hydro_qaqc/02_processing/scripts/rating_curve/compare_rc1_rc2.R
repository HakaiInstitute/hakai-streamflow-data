# =============================================================================
# compare_rc1_rc2.R -- is the RC1/RC2 step a datum offset or a real control change?
# =============================================================================
# Run from the PROJECT ROOT:
#   source("02_processing/scripts/rating_curve/compare_rc1_rc2.R")
#
# Merges rc1vrc2.R + rc1vrc2_shared_exponent.R + rc1vrc2_robustness.R. Set
# TESTS and WINDOW at the top.
#
# Model:  Q = a * (h - h0)^b
#   m_shared    -- b shared across RC1/RC2, a & h0 free per period
#                  (null: same hydraulic control, different datum)
#   m_separate  -- a, h0, b all free per period
#   anova(m_shared, m_separate): significant => exponents genuinely differ.
#   The fitted h0 gap (h0_RC2 - h0_RC1) from m_shared is the implied datum
#   offset. Since RC1 is now on MK's ssn703_a datum (prep_gaugings.R -- no
#   +2 cm), that number is directly interpretable.
#
# Inputs:  03_docs/metadata/ssn703_gaugings_prepped.csv
# Outputs: 02_processing/plots/ssn703_rc1_rc2_<test>.pdf + console tables
# =============================================================================

library(tidyverse)
library(boot)

if (!requireNamespace("minpack.lm", quietly = TRUE)) {
  stop("compare_rc1_rc2.R needs the 'minpack.lm' package (nlsLM). ",
       "Install it with: install.packages('minpack.lm')")
}
library(minpack.lm)

META_DIR <- "03_docs/metadata"
PLOT_DIR <- "02_processing/plots"
dir.create(PLOT_DIR, showWarnings = FALSE, recursive = TRUE)

# -- config --------------------------------------------------------------------
TESTS <- c("shared_exponent", "robustness_loo", "window_sensitivity", "residuals")
# Fitting window (PST). Full history = c(NA, NA). The tight transition window
# c("2017-01-01","2020-01-01") removes multi-sensor / multi-year pooling.
WINDOW      <- c("2017-01-01", "2020-01-01")
BOOT_R      <- 1000
set.seed(703)


# -----------------------------------------------------------------------------
# Load + filter
# -----------------------------------------------------------------------------
gaugings <- read_csv(file.path(META_DIR, "ssn703_gaugings_prepped.csv"), show_col_types = FALSE)

filter_window <- function(g, win) {
  g <- g |> filter(rating_curve_period %in% c("RC1", "RC2"),
                   stage_status == "ok",
                   !is.na(Stage_avg_corrected), !is.na(Q_meas),
                   # Per-gauging accept/reject flag, now maintained for RC1 AND
                   # RC2 in ssn703_rating_approvals.csv (prep_gaugings.R writes
                   # it into Final_rating_curve). Keep only approved gaugings.
                   # RC2's seed state == "all stage-ok gaugings except WY2020-
                   # 2021", so this drops the WY2020-2021 RC2 gaugings vs before.
                   Final_rating_curve == "Y")
  if (!is.na(win[1])) g <- g |> filter(datetime >= with_tz(as.POSIXct(win[1], tz = "Etc/GMT+8"), "UTC"))
  if (!is.na(win[2])) g <- g |> filter(datetime <  with_tz(as.POSIXct(win[2], tz = "Etc/GMT+8"), "UTC"))
  g |> mutate(rating_curve_period = factor(rating_curve_period, levels = c("RC1", "RC2")))
}

fit_data <- filter_window(gaugings, WINDOW)
message("Fitting window: ", paste(WINDOW, collapse = " to "))
fit_data |> count(rating_curve_period) |> print()


# -----------------------------------------------------------------------------
# Core: fit both models on a data subset (NULL on failure)
# -----------------------------------------------------------------------------
fit_pair <- function(d) {
  n_by <- d |> count(rating_curve_period) |> pull(n)
  if (length(n_by) < 2 || any(n_by < 5)) return(NULL)

  h0s <- d |> group_by(rating_curve_period) |>
    summarise(g = min(Stage_avg_corrected) - 5, .groups = "drop") |>
    pull(g, name = rating_curve_period)

  tryCatch({
    m_shared <- nlsLM(
      Q_meas ~ ifelse(rating_curve_period == "RC1", a1, a2) *
        pmax(Stage_avg_corrected - ifelse(rating_curve_period == "RC1", h01, h02), 1e-6)^b,
      data = d,
      start = list(a1 = 1, a2 = 1, h01 = unname(h0s["RC1"]), h02 = unname(h0s["RC2"]), b = 1.5),
      control = nls.lm.control(maxiter = 500))
    m_sep <- nlsLM(
      Q_meas ~ ifelse(rating_curve_period == "RC1", a1, a2) *
        pmax(Stage_avg_corrected - ifelse(rating_curve_period == "RC1", h01, h02), 1e-6)^
        ifelse(rating_curve_period == "RC1", b1, b2),
      data = d,
      start = list(a1 = 1, a2 = 1, h01 = unname(h0s["RC1"]), h02 = unname(h0s["RC2"]),
                   b1 = 1.5, b2 = 1.5),
      control = nls.lm.control(maxiter = 500))
    av <- anova(m_shared, m_sep)
    list(m_shared = m_shared, m_sep = m_sep,
         p_value = av[["Pr(>F)"]][2],
         b_shared = unname(coef(m_shared)["b"]),
         b1 = unname(coef(m_sep)["b1"]), b2 = unname(coef(m_sep)["b2"]),
         offset = unname(coef(m_shared)["h02"] - coef(m_shared)["h01"]),
         n_rc1 = n_by[1], n_rc2 = n_by[2])
  }, error = function(e) NULL)
}

base_fit <- fit_pair(fit_data)
if (is.null(base_fit)) stop("Base model fit failed -- widen WINDOW or check the data.")


# -----------------------------------------------------------------------------
# TEST: shared_exponent (the headline result + bootstrap CI on the offset)
# -----------------------------------------------------------------------------
if ("shared_exponent" %in% TESTS) {
  message("\n=== shared vs separate exponent ===")
  print(anova(base_fit$m_shared, base_fit$m_sep))
  message("p = ", round(base_fit$p_value, 4), " -- ",
          if (base_fit$p_value < 0.05) "SIGNIFICANT: exponents differ"
          else "NOT significant: consistent with shared control + datum offset")
  message("shared b = ", round(base_fit$b_shared, 3),
          " | separate b1 = ", round(base_fit$b1, 3), ", b2 = ", round(base_fit$b2, 3))
  message("implied datum offset (h0_RC2 - h0_RC1) = ", round(base_fit$offset, 2), " cm")

  boot_offset <- function(data, idx) {
    f <- fit_pair(data[idx, ]); if (is.null(f)) NA_real_ else f$offset
  }
  message("bootstrap (R = ", BOOT_R, ") ...")
  bo <- boot(fit_data, boot_offset, R = BOOT_R)
  message("failed replicates: ", sum(is.na(bo$t)), " / ", BOOT_R)
  ci <- tryCatch(boot.ci(bo, type = "perc"), error = function(e) NULL)
  if (!is.null(ci)) print(ci)

  sr <- seq(min(fit_data$Stage_avg_corrected), max(fit_data$Stage_avg_corrected), length.out = 200)
  co <- coef(base_fit$m_shared)
  pred <- bind_rows(
    tibble(rating_curve_period = "RC1", Stage_avg_corrected = sr,
           Q_pred = co["a1"] * pmax(sr - co["h01"], 0)^co["b"]),
    tibble(rating_curve_period = "RC2", Stage_avg_corrected = sr,
           Q_pred = co["a2"] * pmax(sr - co["h02"], 0)^co["b"])
  )
  p <- ggplot() +
    geom_point(data = fit_data, aes(Stage_avg_corrected, Q_meas, colour = rating_curve_period),
               alpha = 0.6, size = 2) +
    geom_line(data = pred, aes(Stage_avg_corrected, Q_pred, colour = rating_curve_period), linewidth = 0.8) +
    scale_colour_manual(values = c(RC1 = "#E41A1C", RC2 = "#4DAF4A")) +
    labs(title = "SSN703 -- RC1 vs RC2, shared-exponent fit",
         subtitle = paste0("shared b = ", round(base_fit$b_shared, 3),
                           " | implied offset = ", round(base_fit$offset, 2), " cm",
                           " | ANOVA p = ", round(base_fit$p_value, 4),
                           " | n = ", nrow(fit_data)),
         x = "Stage corrected (cm)", y = "Discharge (m3/s)", colour = "RC period",
         caption = "RC1 on the ssn703_a (MK) datum -- rating_curve/compare_rc1_rc2.R") +
    theme_bw() + theme(legend.position = "bottom")
  ggsave(file.path(PLOT_DIR, "ssn703_rc1_rc2_shared_exponent.pdf"), p, width = 10, height = 7)
}


# -----------------------------------------------------------------------------
# TEST: robustness_loo (does one point drive the result?)
# -----------------------------------------------------------------------------
if ("robustness_loo" %in% TESTS) {
  message("\n=== leave-one-out (refits ", nrow(fit_data), " times) ===")
  d <- fit_data |> mutate(row_id = row_number())
  loo <- map_dfr(d$row_id, function(i) {
    f <- fit_pair(d |> filter(row_id != i))
    tibble(row_id = i,
           p_value = if (is.null(f)) NA_real_ else f$p_value,
           b1 = if (is.null(f)) NA_real_ else f$b1,
           b2 = if (is.null(f)) NA_real_ else f$b2)
  }) |>
    left_join(d |> select(row_id, EventID, MID, datetime, rating_curve_period,
                          Stage_avg_corrected, Q_meas), by = "row_id")

  n_flip <- sum(loo$p_value >= 0.05, na.rm = TRUE)
  message("refits where removing ONE point flips to non-significant: ", n_flip,
          " / ", sum(!is.na(loo$p_value)))
  if (n_flip > 0) loo |> filter(p_value >= 0.05) |>
    select(EventID, MID, datetime, rating_curve_period, Stage_avg_corrected, Q_meas, p_value) |> print()
  message("b1 range: ", round(min(loo$b1, na.rm = TRUE), 3), " to ", round(max(loo$b1, na.rm = TRUE), 3),
          " | b2 range: ", round(min(loo$b2, na.rm = TRUE), 3), " to ", round(max(loo$b2, na.rm = TRUE), 3))

  p <- loo |> filter(!is.na(p_value)) |>
    mutate(lbl = paste0(rating_curve_period, ": ", round(Stage_avg_corrected), "cm")) |>
    ggplot(aes(reorder(lbl, p_value), p_value, colour = rating_curve_period)) +
    geom_point() + geom_hline(yintercept = 0.05, linetype = "dashed", colour = "red") +
    scale_colour_manual(values = c(RC1 = "#E41A1C", RC2 = "#4DAF4A")) +
    labs(title = "Leave-one-out: ANOVA p-value with each gauging removed",
         x = "gauging removed", y = "p-value") +
    theme_bw() + theme(axis.text.x = element_blank(), axis.ticks.x = element_blank(),
                       legend.position = "bottom")
  ggsave(file.path(PLOT_DIR, "ssn703_rc1_rc2_robustness_loo.pdf"), p, width = 10, height = 6)
}


# -----------------------------------------------------------------------------
# TEST: window_sensitivity
# -----------------------------------------------------------------------------
if ("window_sensitivity" %in% TESTS) {
  message("\n=== window sensitivity ===")
  windows <- tribble(
    ~label,              ~start,        ~end,
    "2016-2021 (wide)",  "2016-01-01",  "2021-01-01",
    "2017-2020 (base)",  "2017-01-01",  "2020-01-01",
    "2018-2021 (shift)", "2018-01-01",  "2021-01-01",
    "2017-2019 (tight)", "2017-01-01",  "2019-06-01"
  )
  ws <- windows |>
    mutate(fit = map2(start, end, ~ fit_pair(filter_window(gaugings, c(.x, .y))))) |>
    mutate(
      n_rc1   = map_dbl(fit, ~ .x$n_rc1  %||% NA),
      n_rc2   = map_dbl(fit, ~ .x$n_rc2  %||% NA),
      b1      = map_dbl(fit, ~ .x$b1     %||% NA),
      b2      = map_dbl(fit, ~ .x$b2     %||% NA),
      offset  = map_dbl(fit, ~ .x$offset %||% NA),
      p_value = map_dbl(fit, ~ .x$p_value %||% NA)
    ) |>
    select(label, n_rc1, n_rc2, b1, b2, offset, p_value)
  print(ws)
  message("significant (p<0.05) in ", sum(ws$p_value < 0.05, na.rm = TRUE), " of ",
          sum(!is.na(ws$p_value)), " windows")
}


# -----------------------------------------------------------------------------
# TEST: residuals (model misspecification?)
# -----------------------------------------------------------------------------
if ("residuals" %in% TESTS) {
  message("\n=== residual diagnostics (separate-exponent model) ===")
  rd <- fit_data |>
    mutate(fitted = fitted(base_fit$m_sep), resid = residuals(base_fit$m_sep),
           std_resid = resid / sd(resid))
  p1 <- ggplot(rd, aes(Stage_avg_corrected, std_resid, colour = rating_curve_period)) +
    geom_point(size = 2) + geom_hline(yintercept = c(-2, 0, 2), linetype = c("dotted", "dashed", "dotted")) +
    scale_colour_manual(values = c(RC1 = "#E41A1C", RC2 = "#4DAF4A")) +
    labs(title = "Standardized residuals vs stage", x = "Stage corrected (cm)", y = "std residual") +
    theme_bw() + theme(legend.position = "bottom")
  p2 <- ggplot(rd, aes(datetime, std_resid, colour = rating_curve_period)) +
    geom_point(size = 2) + geom_hline(yintercept = c(-2, 0, 2), linetype = c("dotted", "dashed", "dotted")) +
    scale_colour_manual(values = c(RC1 = "#E41A1C", RC2 = "#4DAF4A")) +
    labs(title = "Standardized residuals vs time", x = NULL, y = "std residual") +
    theme_bw() + theme(legend.position = "bottom")
  pdf(file.path(PLOT_DIR, "ssn703_rc1_rc2_residuals.pdf"), width = 10, height = 10)
  print(p1); print(p2); dev.off()
  n_big <- sum(abs(rd$std_resid) > 2)
  message(n_big, " gaugings with |std residual| > 2")
  if (n_big > 0) rd |> filter(abs(std_resid) > 2) |>
    select(EventID, MID, datetime, rating_curve_period, Stage_avg_corrected, Q_meas, std_resid) |> print()
}

message("\nDone. 'not significant' supports but does not prove shared control -- ",
        "cross-check surprising results against field notes.")
