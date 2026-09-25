# =============================================================================
# SSN703 Stage QC - Script 07: Rating Curve Exploration
# =============================================================================
# Purpose:
#   Produce exploratory plots to inform rating curve fitting decisions.
#   Two output formats:
#   1. Interactive HTML (plotly) -- for zooming, hovering, toggling water years
#   2. Static PDF (ggplot2) -- archival record of the same plots
#
# This script is intentionally generic -- it reads station_id and RC periods
# from the gauging table and adapts automatically. Suitable for other sites
# with minimal changes.
#
# Inputs:
#   03_docs/metadata/ssn703_gaugings_prepped.csv
#
# Outputs:
#   02_processing/plots/ssn703_rating_curve_exploration.html
#   02_processing/plots/ssn703_rating_curve_exploration.pdf
#
# Plot contents:
#   Plot 1: all RC periods, stage vs discharge, coloured by RC period
#   Plot 2: stage vs discharge coloured by water year, faceted by RC period
#   Plot 3: RC2 only, stage vs discharge coloured by water year
#
# Notes:
#   - stage_suspect and stage_missing gaugings are excluded from all plots
#   - Stage_avg_corrected is used throughout (offset already applied in 06)
#   - Water year from WY column
#   - Hover text includes EventID/MID, date, Q_meas, stage, method, WY
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(lubridate)
library(plotly)
library(htmlwidgets)


# -----------------------------------------------------------------------------
# 0. Setup
# -----------------------------------------------------------------------------

meta_dir <- "03_docs/metadata"
plot_dir <- "02_processing/plots"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)

# RC period colours -- consistent across all plots
rc_colours <- c(
  "RC1" = "#E41A1C",
  "RC2" = "#4DAF4A",
  "RC3" = "#984EA3"
)


# -----------------------------------------------------------------------------
# 1. Load gauging table
# -----------------------------------------------------------------------------

gaugings <- read_csv(
  file.path(meta_dir, "ssn703_gaugings_prepped.csv"),
  show_col_types = FALSE
) |>
  filter(stage_status == "ok") |>   # exclude stage_missing and stage_suspect
  filter(!is.na(Q_meas))            # exclude gaugings with no discharge

station_id <- unique(gaugings$SiteID)
rc_periods <- sort(unique(gaugings$rating_curve_period))

message("Loaded ", nrow(gaugings), " gaugings for station ", station_id)
message("RC periods present: ", paste(rc_periods, collapse = ", "))


# -----------------------------------------------------------------------------
# 2. Prepare hover text
# -----------------------------------------------------------------------------

gaugings <- gaugings |>
  mutate(
    # Identifier -- use EventID if available, else MID
    gauge_id = case_when(
      !is.na(EventID) ~ paste0("EventID: ", EventID),
      !is.na(MID)     ~ paste0("MID: ", MID),
      TRUE            ~ "ID: unknown"
    ),
    hover_text = paste0(
      gauge_id, "<br>",
      "Date: ", Date, "<br>",
      "WY: ", WY, "<br>",
      "Method: ", Method, "<br>",
      "RC period: ", rating_curve_period, "<br>",
      "Stage corrected: ", round(Stage_avg_corrected, 1), " cm<br>",
      "Q: ", round(Q_meas, 3), " m³/s<br>",
      "Q uncertainty: ", Q_rel_unc, "%"
    )
  )


# -----------------------------------------------------------------------------
# 3. Water year colour palette
# -----------------------------------------------------------------------------
# Generate a colour palette that works for any number of water years

wy_levels <- sort(unique(gaugings$WY))
n_wy      <- length(wy_levels)

# Use a perceptually uniform palette that handles many categories
wy_colours <- setNames(
  colorRampPalette(c("#2166AC", "#4DAF4A", "#FF7F00", "#E41A1C", "#984EA3"))(n_wy),
  wy_levels
)


# -----------------------------------------------------------------------------
# 4. Build plotly plots
# -----------------------------------------------------------------------------

# Helper: shared axis and layout settings
base_layout <- function(p, title, subtitle = NULL) {
  p |>
    layout(
      title = list(
        text = if (!is.null(subtitle))
          paste0("<b>", title, "</b><br><sup>", subtitle, "</sup>")
        else
          paste0("<b>", title, "</b>"),
        font = list(size = 14)
      ),
      xaxis = list(title = "Stage corrected (cm)", zeroline = FALSE),
      yaxis = list(title = "Discharge (m³/s)", zeroline = FALSE),
      legend = list(orientation = "v", x = 1.02, y = 1),
      margin = list(r = 150),
      hovermode = "closest"
    )
}


# -- Plot 1: All RC periods, coloured by RC period ---------------------------

p1 <- plot_ly()

for (rc in rc_periods) {
  df_rc <- gaugings |> filter(rating_curve_period == rc)
  p1 <- p1 |>
    add_trace(
      data        = df_rc,
      x           = ~Stage_avg_corrected,
      y           = ~Q_meas,
      type        = "scatter",
      mode        = "markers",
      name        = rc,
      marker      = list(
        color  = rc_colours[rc],
        size   = 8,
        opacity = 0.8,
        symbol = ~case_when(
          Method == "Autosalt"    ~ "circle",
          Method == "Flow Tracker" ~ "triangle-up",
          Method == "Propeller"   ~ "square",
          Method == "Salt"        ~ "cross",
          TRUE                    ~ "circle"
        )
      ),
      text        = ~hover_text,
      hoverinfo   = "text"
    )
}

p1 <- p1 |>
  base_layout(
    title    = paste0(station_id, " -- Stage vs discharge by RC period"),
    subtitle = "Coloured by RC period | hover for gauging details"
  )


# -- Plot 2: Coloured by water year, faceted by RC period --------------------
# plotly doesn't facet natively -- use subplots

subplot_list <- list()

for (rc in rc_periods) {
  df_rc <- gaugings |> filter(rating_curve_period == rc)
  p_sub <- plot_ly()
  
  for (wy in sort(unique(df_rc$WY))) {
    df_wy <- df_rc |> filter(WY == wy)
    p_sub <- p_sub |>
      add_trace(
        data      = df_wy,
        x         = ~Stage_avg_corrected,
        y         = ~Q_meas,
        type      = "scatter",
        mode      = "markers",
        name      = wy,
        legendgroup = wy,
        showlegend  = (rc == rc_periods[1]),  # only show legend for first panel
        marker    = list(
          color   = wy_colours[wy],
          size    = 8,
          opacity = 0.8
        ),
        text      = ~hover_text,
        hoverinfo = "text"
      )
  }
  
  p_sub <- p_sub |>
    layout(
      annotations = list(list(
        text      = rc,
        x         = 0.5,
        y         = 1.05,
        xref      = "paper",
        yref      = "paper",
        showarrow = FALSE,
        font      = list(size = 12, color = rc_colours[rc])
      ))
    )
  
  subplot_list[[rc]] <- p_sub
}

p2 <- subplot(subplot_list, nrows = 1, shareY = FALSE, titleX = TRUE) |>
  layout(
    title = list(
      text = paste0("<b>", station_id,
                    " -- Stage vs discharge by water year, faceted by RC period</b>",
                    "<br><sup>Coloured by water year | hover for gauging details</sup>"),
      font = list(size = 14)
    ),
    hovermode = "closest",
    margin    = list(r = 150)
  )


# -- Plot 3: RC2 only, coloured by water year --------------------------------

df_rc2 <- gaugings |> filter(rating_curve_period == "RC2")

p3 <- plot_ly()

for (wy in sort(unique(df_rc2$WY))) {
  df_wy <- df_rc2 |> filter(WY == wy)
  p3 <- p3 |>
    add_trace(
      data      = df_wy,
      x         = ~Stage_avg_corrected,
      y         = ~Q_meas,
      type      = "scatter",
      mode      = "markers",
      name      = wy,
      marker    = list(
        color   = wy_colours[wy],
        size    = 8,
        opacity = 0.8
      ),
      text      = ~hover_text,
      hoverinfo = "text"
    )
}

p3 <- p3 |>
  base_layout(
    title    = paste0(station_id, " -- RC2: Stage vs discharge by water year"),
    subtitle = "RC2 only | coloured by water year | use to assess temporal drift or geomorphological change"
  )


# -----------------------------------------------------------------------------
# 5. Save HTML
# -----------------------------------------------------------------------------
# Combine all three plots into one HTML file with titles as separators

html_path <- file.path(plot_dir, paste0(tolower(station_id),
                                        "_rating_curve_exploration.html"))

combined <- subplot(
  list(p1, p3),
  nrows  = 2,
  heights = c(0.45, 0.55),
  margin  = 0.08
) |>
  layout(
    title = list(
      text = paste0("<b>", station_id, " Rating Curve Exploration</b>"),
      font = list(size = 16)
    )
  )

# Save p2 (faceted) separately since subplot-of-subplots is complex
html_p1_p3 <- file.path(plot_dir, paste0(tolower(station_id),
                                         "_rc_exploration_overview.html"))
html_p2    <- file.path(plot_dir, paste0(tolower(station_id),
                                         "_rc_exploration_by_wy_faceted.html"))

saveWidget(combined, html_p1_p3, selfcontained = TRUE)
saveWidget(p2,       html_p2,    selfcontained = TRUE)

message("Saved HTML (overview): ", html_p1_p3)
message("Saved HTML (faceted by WY): ", html_p2)


# -----------------------------------------------------------------------------
# 6. Save PDF (static ggplot2 version)
# -----------------------------------------------------------------------------

theme_rc <- function() {
  theme_bw() +
    theme(
      legend.position  = "bottom",
      panel.grid.minor = element_blank(),
      plot.caption     = element_text(hjust = 0, size = 8, colour = "grey40"),
      plot.subtitle    = element_text(size = 9, colour = "grey40")
    )
}

# Page 1: all RC periods
g1 <- gaugings |>
  ggplot(aes(x = Stage_avg_corrected, y = Q_meas,
             colour = rating_curve_period, shape = Method)) +
  geom_point(size = 2, alpha = 0.8, na.rm = TRUE) +
  scale_colour_manual(values = rc_colours) +
  labs(
    title    = paste0(station_id, " -- Stage vs discharge by RC period"),
    subtitle = "Coloured by RC period",
    x        = "Stage corrected (cm)",
    y        = "Discharge (m³/s)",
    colour   = "RC period",
    caption  = "Plot generated by 07_rating_curve_exploration.R"
  ) +
  theme_rc()

# Page 2: coloured by water year, faceted by RC period
g2 <- gaugings |>
  ggplot(aes(x = Stage_avg_corrected, y = Q_meas,
             colour = WY, shape = Method)) +
  geom_point(size = 2, alpha = 0.8, na.rm = TRUE) +
  facet_wrap(~ rating_curve_period, scales = "free") +
  scale_colour_manual(values = wy_colours) +
  labs(
    title    = paste0(station_id, " -- Stage vs discharge by water year"),
    subtitle = "Faceted by RC period | coloured by water year",
    x        = "Stage corrected (cm)",
    y        = "Discharge (m³/s)",
    colour   = "Water year",
    caption  = "Plot generated by 07_rating_curve_exploration.R"
  ) +
  theme_rc() +
  guides(colour = guide_legend(ncol = 4))

# Page 3: RC2 only, coloured by water year
g3 <- gaugings |>
  filter(rating_curve_period == "RC2") |>
  ggplot(aes(x = Stage_avg_corrected, y = Q_meas,
             colour = WY, shape = Method)) +
  geom_point(size = 2, alpha = 0.8, na.rm = TRUE) +
  scale_colour_manual(values = wy_colours) +
  labs(
    title    = paste0(station_id, " -- RC2: Stage vs discharge by water year"),
    subtitle = "RC2 only | use to assess temporal drift or geomorphological change",
    x        = "Stage corrected (cm)",
    y        = "Discharge (m³/s)",
    colour   = "Water year",
    caption  = "Plot generated by 07_rating_curve_exploration.R"
  ) +
  theme_rc() +
  guides(colour = guide_legend(ncol = 4))

pdf_path <- file.path(plot_dir, paste0(tolower(station_id),
                                       "_rating_curve_exploration.pdf"))
pdf(pdf_path, width = 12, height = 8)
print(g1)
print(g2)
print(g3)
dev.off()

message("Saved PDF: ", pdf_path)
message("\nDone -- review plots before proceeding to rating curve fitting")
message("Key questions to answer from these plots:")
message("  1. Is there evidence of temporal drift in RC2 (page/plot 3)?")
message("  2. Are there outlier gaugings that should be excluded?")
message("  3. Does each RC period have sufficient coverage across the flow range?")