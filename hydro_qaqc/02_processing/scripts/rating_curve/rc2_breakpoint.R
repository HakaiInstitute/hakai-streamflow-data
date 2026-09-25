# =============================================================================
# rc2_breakpoint.R -- pin the date of the RC2 stage-discharge shift at loc_2
# =============================================================================
# Run from the PROJECT ROOT:
#   source("02_processing/scripts/rating_curve/rc2_breakpoint.R")
#
# Context (memory/ssn703-rc2-temporal-shift.md): RC2 gaugings drift from the
# "old" rating to a curve ~x1.3 higher in Q (roughly stage-independent; a
# stage/datum offset does NOT explain it -> a conveyance / section-enlargement
# change, consistent with the bank detachment Emily reports near an upstream
# riffle that also loads with debris/logs).
#
# Three lines of evidence, because the gaugings alone can't resolve it (269-day
# gap 2020-12-10 -> 2021-09-05):
#   A. Continuous low-flow stage baseline (monthly p05/p10 + daily rolling) --
#      a section enlargement steps the dry-weather stage DOWN and it stays down.
#      THIS is what pins the date; the gaugings only bracket it.
#   B. Gauging residuals vs the pre-shift curve, over time -- corroboration.
#   C. Gauging change-point scan (shared power-law shape, one multiplicative Q
#      step) -- reports the best split AND its irreducible bracket.
#
# Candidate triggers: 2020-11-27 flood (218 cm, highest on record), 2021-02-21
# flood (212 cm, 2nd; rain-on-snow), and the late-Jun-2021 heat dome. The site
# has only modest snowpack, so "freshet" is not the story.
#
# FINDING (2026-09-10): the shift is real (gauging residuals, climate-immune) and
# falls in the Dec-2020 -> Sep-2021 gauging gap. The Nov-2020 flood is ruled out
# (Dec-2020 gaugings still on the old rating). Feb-2021 flood vs Jun-2021 heat
# dome can't be separated -- no gaugings, no survey. The low-flow stage step is
# on the heat-dome week and partly reflects the 2021-23 drought; winter baseflow
# is the less-confounded signal. Nominal split date 2021-02-21.
#
# Outputs -> 02_processing/plots/rc2_breakpoint_*.png  + a console summary.
# =============================================================================

library(tidyverse)
library(lubridate)

source("02_processing/scripts/rating_curve/rating_curve_functions.R")

PLOT_DIR <- "02_processing/plots"
dir.create(PLOT_DIR, showWarnings = FALSE, recursive = TRUE)

RC2_START <- as.Date("2018-09-14")
RC2_END   <- as.Date("2023-06-25")
FLOODS    <- as.Date(c("2020-11-27", "2021-02-21"))   # 218 cm / 212 cm -- 1st & 2nd on record
HEATDOME  <- as.Date(c("2021-06-25", "2021-07-02"))   # W. North America record heat event
# NOTE ON THE CONFOUND: the June-2021 low-flow drop coincides with the heat dome
# AND the onset of the 2021-2023 BC drought -- so the summer low-flow step is
# part real channel change, part climate. Winter baseflow (Dec-Feb, not driven
# by summer ET / snowmelt) is the less-confounded check; the gauging residual
# shift is climate-immune (same stage -> more Q regardless of how wet the year).

# -----------------------------------------------------------------------------
# Load
# -----------------------------------------------------------------------------
stage_c <- read_csv("04_outputs/per_sensor/ssn703_ssn703_c_stage_qc.csv",
                    show_col_types = FALSE, guess_max = 3e5) |>
  mutate(ts = parse_date_time(timestamp, orders = c("Ymd HMS", "Ymd"), tz = "UTC")) |>
  filter(!is.na(ts), qc_flag %in% c("raw", "gf_spline", "gf_sa"),
         !is.na(stage_corrected)) |>
  transmute(date = as_date(ts), stage_cm = stage_corrected * 100) |>
  filter(stage_cm > 0, stage_cm < 400)          # drop fill/sentinel garbage

gaugings <- read_csv("03_docs/metadata/ssn703_gaugings_prepped.csv",
                     show_col_types = FALSE) |>
  filter(rating_curve_period == "RC2", Final_rating_curve == "Y",
         !is.na(Q_meas), !is.na(Stage_avg_corrected)) |>
  transmute(date = as_date(datetime), WY,
            Stage_avg = Stage_avg_corrected, Q_meas, Q_rel_unc) |>
  arrange(date)

g <- sort(gaugings$date)
big_gap <- { i <- which.max(diff(g)); c(g[i], g[i + 1]) }
# the gap that matters here: the one straddling the suspected shift
POST_GAP <- c(max(g[g <= "2021-01-01"]), min(g[g >= "2021-06-01"]))
message(nrow(gaugings), " RC2 gaugings; largest gap ", format(big_gap[1]), " -> ",
        format(big_gap[2]), " (", as.numeric(diff(big_gap)), " d); ",
        "post-2020 gap ", format(POST_GAP[1]), " -> ", format(POST_GAP[2]),
        " (", as.numeric(diff(POST_GAP)), " d)")

# -----------------------------------------------------------------------------
# A. Continuous low-flow stage baseline
# -----------------------------------------------------------------------------
monthly_low <- stage_c |>
  mutate(ym = floor_date(date, "month"), mo = month(date), yr = year(date)) |>
  group_by(ym, mo, yr) |>
  summarise(nd = n_distinct(date),
            p05 = quantile(stage_cm, 0.05, names = FALSE),
            p10 = quantile(stage_cm, 0.10, names = FALSE), .groups = "drop") |>
  filter(nd >= 20, ym >= RC2_START, ym <= RC2_END)

# deseasonalise: anomaly of monthly p05 vs the 2019-2020 mean for that calendar
# month (the pre-shift baseline). Lets the step search work at monthly
# resolution instead of once per summer.
seas_base <- monthly_low |> filter(yr %in% c(2019, 2020)) |>
  group_by(mo) |> summarise(base_p05 = mean(p05), .groups = "drop")
monthly_low <- monthly_low |> left_join(seas_base, by = "mo") |>
  mutate(anom_cm = p05 - base_p05)

daily_min <- stage_c |>
  group_by(date) |>
  summarise(smin = min(stage_cm), .groups = "drop") |>
  complete(date = seq(min(date), max(date), by = "day"))
roll_q <- function(x, k = 45, p = 0.1) vapply(seq_along(x), function(i) {
  w <- x[max(1, i - k + 1):i]; w <- w[!is.na(w)]
  if (length(w) < k / 3) NA_real_ else quantile(w, p, names = FALSE)
}, numeric(1))
daily_min <- daily_min |> mutate(roll = roll_q(smin))

# monthly step search on the deseasonalised anomaly
cand <- monthly_low |> filter(ym >= "2020-06-01", ym <= "2022-03-01") |> pull(ym)
step_scan <- map_dfr(cand, function(cut) {
  a <- monthly_low |> filter(ym >= "2020-01-01")
  b4 <- a$anom_cm[a$ym <  cut]; af <- a$anom_cm[a$ym >= cut]
  if (length(b4) < 3 || length(af) < 3) return(NULL)
  tibble(cut = cut, mean_before = mean(b4), mean_after = mean(af),
         drop_cm = mean(b4) - mean(af))
})
best_step <- step_scan |> slice_max(drop_cm, n = 1, with_ties = FALSE)

# daily-resolution timing: first sustained break below 44 cm in the roll-p10
dstep <- daily_min |> filter(date >= "2021-03-01", date <= "2021-09-01", !is.na(roll))
day_break <- dstep$date[which(dstep$roll < 44)][1]

message("\n-- A. low-flow stage step (deseasonalised monthly p05 anomaly) --")
message("  largest month-scale step at ", format(best_step$cut, "%b %Y"), ":  anomaly ",
        round(best_step$mean_before, 1), " -> ", round(best_step$mean_after, 1), " cm")
message("  daily roll-p10 first drops below 44 cm around ", format(day_break),
        "  (== the heat-dome week, ", format(HEATDOME[1]), " -- confounded)")
message("  monthly p05 anomaly (cm) vs 2019-2020 baseline:")
monthly_low |> filter(ym >= "2020-06-01", ym <= "2022-03-01") |>
  transmute(month = format(ym, "%Y-%m"), p05 = round(p05, 1), anom_cm = round(anom_cm, 1)) |>
  as.data.frame() |> print(row.names = FALSE)

# less-confounded check: winter (Dec-Feb) baseflow stage -- not driven by
# summer ET or snowmelt, so a persistent drop here is likelier a real control change.
winter_bf <- stage_c |>
  filter(month(date) %in% c(12, 1, 2)) |>
  mutate(winter = if_else(month(date) == 12, year(date) + 1L, year(date))) |>
  group_by(winter) |>
  summarise(p10 = round(quantile(stage_cm, 0.10, names = FALSE), 1),
            p25 = round(quantile(stage_cm, 0.25, names = FALSE), 1), .groups = "drop")
message("\n  winter (Dec-Feb) baseflow stage, cm -- the climate-robust check:")
as.data.frame(winter_bf) |> print(row.names = FALSE)
message("  winter 2022 & 2023 p10 sit ~4-7 cm below winter 2020-2021 -> supports a real",
        " low-water-control drop, though within interannual range (winter 2019 was similar).")

p_A <- ggplot() +
  annotate("rect", xmin = HEATDOME[1], xmax = HEATDOME[2], ymin = -Inf, ymax = Inf,
           fill = "#fdae61", alpha = 0.5) +
  annotate("text", x = HEATDOME[1], y = 120, label = "heat dome ", hjust = 1,
           size = 2.9, colour = "#b35806") +
  geom_line(data = filter(daily_min, date >= RC2_START, date <= RC2_END),
            aes(date, smin), colour = "grey82", linewidth = 0.2) +
  geom_line(data = filter(daily_min, date >= RC2_START, date <= RC2_END),
            aes(date, roll), colour = "#1f78b4", linewidth = 0.9) +
  geom_point(data = monthly_low, aes(ym, p05), colour = "#e31a1c", size = 1.5) +
  geom_vline(xintercept = FLOODS, linetype = "dotted", colour = "grey35") +
  geom_vline(xintercept = best_step$cut, linetype = "dashed", colour = "black") +
  annotate("text", x = FLOODS, y = Inf, label = c(" Nov 2020 flood", " Feb 2021 flood"),
           hjust = 0, vjust = c(1.4, 3), size = 2.9, colour = "grey35") +
  annotate("text", x = day_break, y = -Inf,
           label = paste0(" step ~", format(day_break, "%b %Y")),
           hjust = 0, vjust = -0.8, size = 3) +
  geom_rug(data = gaugings, aes(date), sides = "b", colour = "#33a02c", alpha = 0.6) +
  scale_x_date(date_breaks = "6 months", date_labels = "%b %Y") +
  labs(title = "RC2 loc_2 -- continuous low-flow stage baseline (sensor C)",
       subtitle = paste0("blue = 45-day rolling p10 of daily-min stage; red = monthly p05; ",
                         "green ticks = gaugings; dotted = Nov-2020 & Feb-2021 floods\n",
                         "the ~Jun-2021 drop coincides with the heat dome + start of the 2021-23 drought -- ",
                         "partly climate, not all channel change"),
       x = NULL, y = "Stage (cm)") +
  theme_bw() + theme(panel.grid.minor = element_blank())
ggsave(file.path(PLOT_DIR, "rc2_breakpoint_lowflow.png"), p_A, width = 11, height = 5, dpi = 130)

# -----------------------------------------------------------------------------
# B. Gauging residuals vs the pre-shift curve, over time
# -----------------------------------------------------------------------------
pre <- gaugings |> filter(date <= "2020-12-31")     # everything up to & incl. the post-Nov-flood weeks
pre_grid <- rc_loess_curve(pre, 0.5, seq(ceiling(min(pre$Stage_avg)),
                                        floor(max(pre$Stage_avg)), 1))
cfn_pre <- approxfun(pre_grid$Stage_avg, pre_grid$Q_model, rule = 2)

resid_t <- gaugings |>
  mutate(Q_pre = cfn_pre(Stage_avg),
         resid_pct = (Q_meas - Q_pre) / Q_pre * 100)

message("\n-- B. gauging residual vs the pre-shift (<= 2020-12) curve --")
resid_t |>
  mutate(cluster = case_when(
    date <= "2020-11-25" ~ "1  <= 2020-11-25 (pre-Nov-flood)",
    date <= "2020-12-31" ~ "2  Dec 2020 (weeks after Nov flood)",
    date <= "2021-09-30" ~ "3  Sep 2021 (first gaugings after the gap)",
    date <= "2022-03-31" ~ "4  Oct 2021 - Mar 2022",
    TRUE                 ~ "5  Apr - Dec 2022")) |>
  group_by(cluster) |>
  summarise(n = n(), median_resid_pct = round(median(resid_pct), 1), .groups = "drop") |>
  as.data.frame() |> print(row.names = FALSE)

p_B <- ggplot(resid_t, aes(date, resid_pct)) +
  annotate("rect", xmin = POST_GAP[1], xmax = POST_GAP[2], ymin = -Inf, ymax = Inf,
           fill = "grey88") +
  annotate("text", x = mean(POST_GAP), y = 75, label = "no gaugings",
           size = 3, colour = "grey45") +
  geom_hline(yintercept = 0, colour = "grey40") +
  geom_vline(xintercept = FLOODS, linetype = "dotted", colour = "grey35") +
  geom_vline(xintercept = day_break, linetype = "dashed") +
  geom_point(aes(colour = resid_pct > 15), size = 2, alpha = 0.85) +
  scale_colour_manual(values = c(`FALSE` = "#377eb8", `TRUE` = "#e41a1c"),
                      labels = c("on old rating", "shifted (>+15%)"), name = NULL) +
  scale_x_date(date_breaks = "6 months", date_labels = "%b %Y") +
  coord_cartesian(ylim = c(-45, 85)) +
  labs(title = "RC2 -- gauging residual against the pre-shift curve",
       subtitle = paste0("Dec-2020 gaugings (weeks after the Nov flood) still on the old rating; ",
                         "shift appears after the gap. Dashed = low-flow step (~", format(day_break, "%b %Y"),
                         "). y clipped at 85%."),
       x = NULL, y = "(Q_meas - Q_pre) / Q_pre  (%)") +
  theme_bw() + theme(panel.grid.minor = element_blank())
ggsave(file.path(PLOT_DIR, "rc2_breakpoint_residual_time.png"), p_B, width = 11, height = 5, dpi = 130)

# -----------------------------------------------------------------------------
# C. Gauging change-point scan -- one multiplicative Q step
# -----------------------------------------------------------------------------
# Candidate cuts = midpoints between consecutive gauging dates (the only places
# the partition, hence the fit, can change). Shared shape in log space:
#   log Q = log a + step*log(rho) + b*log(stage - h0)
cp_scan <- function(h0) {
  d <- gaugings |> filter(Stage_avg > h0 + 1) |>
    mutate(lh = log(Stage_avg - h0), lq = log(Q_meas))
  ud <- sort(unique(d$date))
  cuts <- ud[-length(ud)] + diff(ud) / 2
  map_dfr(cuts, function(cut) {
    d2 <- d |> mutate(step = as.integer(date > cut))
    if (min(sum(d2$step == 0), sum(d2$step == 1)) < 8) return(NULL)
    if (min(diff(range(d2$lh[d2$step == 0])), diff(range(d2$lh[d2$step == 1]))) < 0.6)
      return(NULL)
    m <- lm(lq ~ lh + step, data = d2)
    tibble(cut = as_date(cut), h0 = h0,
           rho = exp(unname(coef(m)["step"])),
           dev = nrow(d2) * log(sum(resid(m)^2) / nrow(d2)))
  })
}
cp <- map_dfr(c(35, 40, 44), cp_scan)
null_dev <- map_dfr(c(35, 40, 44), function(h0) {
  d <- gaugings |> filter(Stage_avg > h0 + 1)
  m <- lm(log(Q_meas) ~ log(Stage_avg - h0), data = d)
  tibble(h0 = h0, null_dev = nrow(d) * log(sum(resid(m)^2) / nrow(d)))
})
cp <- cp |> left_join(null_dev, by = "h0") |> mutate(dev_drop = null_dev - dev)

best_cp <- cp |> filter(h0 == 40) |> slice_max(dev_drop, n = 1)
# bracket = last gauging before the best cut .. first gauging after it
lo <- max(gaugings$date[gaugings$date <  best_cp$cut])
hi <- min(gaugings$date[gaugings$date >= best_cp$cut])
message("\n-- C. gauging change-point scan --")
cp |> group_by(h0) |> slice_max(dev_drop, n = 1) |> ungroup() |>
  transmute(h0, best_cut = cut, rho = round(rho, 2), dev_drop = round(dev_drop, 1)) |>
  as.data.frame() |> print(row.names = FALSE)
message("  best split puts the break between ", format(lo), " and ", format(hi),
        "  -- i.e. inside the gauging gap; the scan cannot localise it further.")

p_C <- ggplot(cp, aes(cut, dev_drop, colour = factor(h0))) +
  annotate("rect", xmin = POST_GAP[1], xmax = POST_GAP[2], ymin = -Inf, ymax = Inf,
           fill = "grey88") +
  geom_line(linewidth = 0.8) + geom_point(size = 1.3) +
  geom_vline(xintercept = FLOODS, linetype = "dotted", colour = "grey35") +
  geom_vline(xintercept = day_break, linetype = "dashed") +
  annotate("text", x = day_break, y = Inf,
           label = paste0(" low-flow step (~", format(day_break, "%b %Y"), ")"),
           hjust = 0, vjust = 1.5, size = 3) +
  scale_x_date(date_breaks = "3 months", date_labels = "%b %Y") +
  labs(title = "RC2 -- gauging change-point profile (higher = stronger break there)",
       subtitle = "flat across the grey gap: with no gaugings there, every cut in the gap gives the same fit",
       x = NULL, y = "deviance drop vs no-break", colour = "h0 (cm)") +
  theme_bw() + theme(panel.grid.minor = element_blank(),
                     axis.text.x = element_text(angle = 30, hjust = 1))
ggsave(file.path(PLOT_DIR, "rc2_breakpoint_changepoint.png"), p_C, width = 11, height = 5, dpi = 130)

# -----------------------------------------------------------------------------
# D. Is there an EARLIER break (2019/2020)? -- the "a tree fell" question
# -----------------------------------------------------------------------------
# Scan for one multiplicative Q step WITHIN the pre-2021 gaugings only, before
# and after removing 4 marginal points: 2018-10-08 ("Suspicious low salt
# volume") and the 3 near-zero-flow propeller gaugings (Q 0.06-0.10 m3/s at
# 52-54 cm -- near the meter floor and near h0, huge leverage in log space).
early_scan <- function(drop_marginal) {
  drop_dates <- as.Date(c("2018-10-08", "2019-05-04", "2019-07-23", "2019-08-14"))
  pre <- gaugings |> filter(date < "2021-01-01")
  if (drop_marginal) pre <- pre |> filter(!date %in% drop_dates)
  map_dfr(c(35, 40, 44), function(h0) {
    d <- pre |> filter(Stage_avg > h0 + 1) |>
      mutate(lh = log(Stage_avg - h0), lq = log(Q_meas))
    m0 <- lm(lq ~ lh, data = d)
    null <- nrow(d) * log(sum(resid(m0)^2) / nrow(d))
    ud <- sort(unique(d$date)); cuts <- ud[-length(ud)] + diff(ud) / 2
    map_dfr(cuts, function(cut) {
      d2 <- d |> mutate(step = as.integer(date > cut))
      if (min(sum(d2$step == 0), sum(d2$step == 1)) < 6) return(NULL)
      if (min(diff(range(d2$lh[d2$step == 0])), diff(range(d2$lh[d2$step == 1]))) < 0.5)
        return(NULL)
      m <- lm(lq ~ lh + step, data = d2)
      tibble(cut = as_date(cut), h0 = h0,
             rho = exp(unname(coef(m)["step"])),
             dev_drop = null - nrow(d2) * log(sum(resid(m)^2) / nrow(d2)))
    })
  }) |> slice_max(dev_drop, n = 1, with_ties = FALSE)
}
e_all  <- early_scan(FALSE)
e_trim <- early_scan(TRUE)
message("\n-- D. earlier-break (2019/2020) check, within the pre-2021 gaugings --")
message("  all pre-2021 points:        best cut ", format(e_all$cut),
        "  rho ", round(e_all$rho, 2), "  dev_drop ", round(e_all$dev_drop, 1))
message("  4 marginal points removed:  best cut ", format(e_trim$cut),
        "  rho ", round(e_trim$rho, 2), "  dev_drop ", round(e_trim$dev_drop, 1))
message("  => the ~Aug-2019 signal is carried by 6 low-quality gaugings (1 flagged dilution",
        " gauging + 3 near-zero propeller points). Remove them and it collapses to noise",
        " (dev_drop ~2 vs ~34 for the 2021 break). No real 2019/2020 shift; the 2019-2020",
        " gaugings plot on one coherent curve.")

message("\nSaved 3 plots -> ", PLOT_DIR, "/rc2_breakpoint_*.png")
message("\nSUMMARY")
message("  A rating shift is REAL: gaugings at the same stage read ~x", round(best_cp$rho, 2),
        " more Q after vs before -- climate cannot cause that.")
message("  Timing: somewhere in the ", as.numeric(hi - lo), "-day gauging gap ",
        format(lo), " .. ", format(hi), ". Two candidate triggers in that window:")
message("    - Feb 2021 rain-on-snow flood (212 cm, 2nd on record) -- classic channel-moving event")
message("    - late-Jun 2021 heat dome + onset of the 2021-23 drought")
message("  Ruled out: the Nov-2020 flood -- the Dec-2020 gaugings (weeks later) are still on the OLD rating.")
message("  The low-flow stage step lands on the heat-dome week, so it is partly climate;")
message("  winter baseflow sitting ~5 cm low in 2022-2023 is the cleaner sign of a real geometry change.")
message("  Mechanism (best guess, no survey): channel-control change -- section scoured/widened or a")
message("  bank failed at/after the Feb-2021 flood; low-water control dropped ~5-10 cm; conveyance up ~30-50%.")
message("  RC2a|RC2b split: nominal 2021-02-21 (apply new rating from the big flood); transition uncertain")
message("  Feb-Jun 2021. No gaugings in the gap, so RC2a is fit <= Dec 2020, RC2b from Sep 2021 either way.")
