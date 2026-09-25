# =============================================================================
# explore_gaugings.R -- exploratory stage vs discharge plots before fitting
# =============================================================================
# Run from the PROJECT ROOT:
#   source("02_processing/scripts/rating_curve/explore_gaugings.R")
#
# Was 07_rating_curve_exploration.R. Reads ssn703_gaugings_prepped.csv,
# produces the same three views (all periods; by water year, faceted; RC2
# drift) as interactive HTML + a static PDF. The palette / hover-text
# boilerplate that was repeated per-plot is now two small helpers.
#
# Inputs:  03_docs/metadata/ssn703_gaugings_prepped.csv
# Outputs: 02_processing/plots/ssn703_rc_exploration.html
#          02_processing/plots/ssn703_rc_exploration_by_wy.html
#          02_processing/plots/ssn703_rating_curve_exploration.pdf
# =============================================================================

library(tidyverse)
library(plotly)
library(htmlwidgets)

META_DIR <- "03_docs/metadata"
PLOT_DIR <- "02_processing/plots"
dir.create(PLOT_DIR, showWarnings = FALSE, recursive = TRUE)

RC_COLOURS <- c(RC1 = "#E41A1C", RC2 = "#4DAF4A", RC3 = "#984EA3")


# -----------------------------------------------------------------------------
# 1. Load + prepare
# -----------------------------------------------------------------------------
gaugings <- read_csv(file.path(META_DIR, "ssn703_gaugings_prepped.csv"), show_col_types = FALSE) |>
  filter(stage_status == "ok", !is.na(Q_meas), !is.na(Stage_avg_corrected)) |>
  mutate(
    gauge_id = if_else(!is.na(EventID), paste0("EventID: ", EventID), paste0("MID: ", MID)),
    hover = paste0(
      gauge_id, "<br>Date: ", Date, " | WY: ", WY,
      "<br>Method: ", Method, " | ", rating_curve_period,
      "<br>Stage: ", round(Stage_avg_corrected, 1), " cm",
      "<br>Q: ", round(Q_meas, 3), " m3/s (+/- ", Q_rel_unc, "%)"
    )
  )

station_id <- unique(gaugings$SiteID)
wy_levels  <- sort(unique(gaugings$WY))
wy_colours <- setNames(
  colorRampPalette(c("#2166AC", "#4DAF4A", "#FF7F00", "#E41A1C", "#984EA3"))(length(wy_levels)),
  wy_levels
)


# -----------------------------------------------------------------------------
# 2. Helpers
# -----------------------------------------------------------------------------
scatter_by <- function(df, colour_col, palette, title, subtitle = NULL) {
  plot_ly(
    df, x = ~Stage_avg_corrected, y = ~Q_meas,
    color = df[[colour_col]], colors = palette,
    type = "scatter", mode = "markers",
    marker = list(size = 8, opacity = 0.8),
    text = ~hover, hoverinfo = "text"
  ) |>
    layout(
      title = list(text = if (is.null(subtitle)) paste0("<b>", title, "</b>")
                   else paste0("<b>", title, "</b><br><sup>", subtitle, "</sup>")),
      xaxis = list(title = "Stage corrected (cm)"),
      yaxis = list(title = "Discharge (m3/s)"),
      legend = list(title = list(text = colour_col))
    )
}

gg_theme <- theme_bw() +
  theme(legend.position = "bottom", panel.grid.minor = element_blank(),
        plot.caption = element_text(hjust = 0, size = 8, colour = "grey40"))


# -----------------------------------------------------------------------------
# 3. Interactive
# -----------------------------------------------------------------------------
p_all <- scatter_by(gaugings, "rating_curve_period", RC_COLOURS,
                    paste0(station_id, " -- stage vs discharge by RC period"),
                    "hover for gauging details")

p_rc2 <- gaugings |>
  filter(rating_curve_period == "RC2") |>
  scatter_by("WY", wy_colours,
             paste0(station_id, " -- RC2 stage vs discharge by water year"),
             "look for temporal drift / geomorphic change")

saveWidget(p_all, file.path(PLOT_DIR, "ssn703_rc_exploration.html"), selfcontained = TRUE)
saveWidget(p_rc2, file.path(PLOT_DIR, "ssn703_rc_exploration_by_wy.html"), selfcontained = TRUE)
message("Saved: ssn703_rc_exploration.html, ssn703_rc_exploration_by_wy.html")


# -----------------------------------------------------------------------------
# 4. Static PDF (archival)
# -----------------------------------------------------------------------------
g1 <- ggplot(gaugings, aes(Stage_avg_corrected, Q_meas,
                           colour = rating_curve_period, shape = Method)) +
  geom_point(size = 2, alpha = 0.8, na.rm = TRUE) +
  scale_colour_manual(values = RC_COLOURS) +
  labs(title = paste0(station_id, " -- stage vs discharge by RC period"),
       x = "Stage corrected (cm)", y = "Discharge (m3/s)", colour = "RC period",
       caption = "rating_curve/explore_gaugings.R") + gg_theme

g2 <- ggplot(gaugings, aes(Stage_avg_corrected, Q_meas, colour = WY, shape = Method)) +
  geom_point(size = 2, alpha = 0.8, na.rm = TRUE) +
  facet_wrap(~ rating_curve_period, scales = "free") +
  scale_colour_manual(values = wy_colours) +
  labs(title = paste0(station_id, " -- stage vs discharge by water year"),
       subtitle = "faceted by RC period", x = "Stage corrected (cm)", y = "Discharge (m3/s)",
       colour = "Water year", caption = "rating_curve/explore_gaugings.R") +
  gg_theme + guides(colour = guide_legend(ncol = 4))

g3 <- gaugings |>
  filter(rating_curve_period == "RC2") |>
  ggplot(aes(Stage_avg_corrected, Q_meas, colour = WY, shape = Method)) +
  geom_point(size = 2, alpha = 0.8, na.rm = TRUE) +
  scale_colour_manual(values = wy_colours) +
  labs(title = paste0(station_id, " -- RC2 stage vs discharge by water year"),
       subtitle = "assess temporal drift / geomorphic change",
       x = "Stage corrected (cm)", y = "Discharge (m3/s)", colour = "Water year",
       caption = "rating_curve/explore_gaugings.R") +
  gg_theme + guides(colour = guide_legend(ncol = 4))

pdf_path <- file.path(PLOT_DIR, "ssn703_rating_curve_exploration.pdf")
pdf(pdf_path, width = 12, height = 8); print(g1); print(g2); print(g3); dev.off()
message("Saved: ", pdf_path)
message("\nKey questions: RC2 temporal drift? outlier gaugings? coverage across the flow range?")
