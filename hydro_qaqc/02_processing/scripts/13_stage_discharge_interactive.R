# =============================================================================
# SSN703 - Interactive Stage-Discharge Curves (combined lookup, all ratings)
# =============================================================================
# Purpose:
#   Plots Q vs Stage directly from the combined rating curve lookup table --
#   NOT a time series, the actual curve SHAPES -- so you can visually inspect
#   how RC1 (Rating 3), RC2 (Rating 4), and RC3 (Rating 5) relate to each
#   other, especially in any stage ranges where they overlap. This is the
#   right view for checking "do the curves transition sensibly" as opposed
#   to script 13's discharge-vs-time view (which shows the sensor boundary
#   transitions over the actual record).
#
#   Uses plotly (via ggplotly), not dygraphs -- this is a stage-discharge
#   relationship, not a time series, so plotly's zoom/hover on an x-y
#   scatter/line is the right fit rather than dygraphs' time-axis strengths.
#
# Inputs:
#   04_outputs/ssn703_rating_curve_lookup_combined.csv
#
# Outputs:
#   02_processing/plots/ssn703_stage_discharge_curves_interactive.html
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)
library(plotly)
library(htmlwidgets)

plot_dir <- "02_processing/plots"
out_dir  <- "04_outputs"
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)


# -----------------------------------------------------------------------------
# 1. Load combined lookup
# -----------------------------------------------------------------------------

lookup <- read_csv(
  file.path(out_dir, "ssn703_rating_curve_lookup_combined.csv"),
  show_col_types = FALSE
)

message("Ratings present in lookup: ")
print(lookup |> count(Rating))

# The three ratings actually applied in production, per the corrected
# sensor/rating boundaries (RC1 = Rating 3, RC2 = Rating 4, RC3 = Rating 5).
# Ratings 1/2 (superseded early RC1 sub-revisions) excluded by default --
# uncomment the line below to include them for historical context instead.
ratings_to_plot <- c(3, 4, 5)
# ratings_to_plot <- c(1, 2, 3, 4, 5)  # <- uncomment for full history including superseded ratings

plot_data <- lookup |>
  filter(Rating %in% ratings_to_plot) |>
  mutate(
    rating_label = case_when(
      Rating == 1 ~ "Rating 1 (superseded)",
      Rating == 2 ~ "Rating 2 (superseded)",
      Rating == 3 ~ "RC1 (Rating 3) -- loc_1",
      Rating == 4 ~ "RC2 (Rating 4) -- loc_2",
      Rating == 5 ~ "RC3 (Rating 5) -- loc_3",
      TRUE ~ paste("Rating", Rating)
    )
  )

message("\nPlotting ratings: ", paste(ratings_to_plot, collapse = ", "))
message("Rows in plot: ", nrow(plot_data))


# -----------------------------------------------------------------------------
# 2. Identify overlapping stage ranges -- worth knowing before looking at
#    the plot, since curve agreement/disagreement in the OVERLAP region is
#    the most informative part
# -----------------------------------------------------------------------------

ranges <- plot_data |>
  group_by(rating_label) |>
  summarise(min_stage = min(Stage_avg), max_stage = max(Stage_avg), .groups = "drop")

message("\nStage range per curve:")
print(ranges)

# Pairwise overlap check between consecutive ratings
if (all(c(3, 4) %in% ratings_to_plot)) {
  r3 <- ranges |> filter(rating_label == "RC1 (Rating 3) -- loc_1")
  r4 <- ranges |> filter(rating_label == "RC2 (Rating 4) -- loc_2")
  overlap_34 <- c(max(r3$min_stage, r4$min_stage), min(r3$max_stage, r4$max_stage))
  if (overlap_34[1] < overlap_34[2]) {
    message("RC1/RC2 overlap: ", round(overlap_34[1], 1), " to ", round(overlap_34[2], 1), " cm")
  } else {
    message("RC1/RC2: NO overlapping stage range")
  }
}
if (all(c(4, 5) %in% ratings_to_plot)) {
  r4 <- ranges |> filter(rating_label == "RC2 (Rating 4) -- loc_2")
  r5 <- ranges |> filter(rating_label == "RC3 (Rating 5) -- loc_3")
  overlap_45 <- c(max(r4$min_stage, r5$min_stage), min(r4$max_stage, r5$max_stage))
  if (overlap_45[1] < overlap_45[2]) {
    message("RC2/RC3 overlap: ", round(overlap_45[1], 1), " to ", round(overlap_45[2], 1), " cm")
  } else {
    message("RC2/RC3: NO overlapping stage range")
  }
}


# -----------------------------------------------------------------------------
# 3. Build interactive plot
# -----------------------------------------------------------------------------

curve_colours <- c(
  "Rating 1 (superseded)"    = "#E07B54",
  "Rating 2 (superseded)"    = "#F0997B",
  "RC1 (Rating 3) -- loc_1"  = "#E41A1C",
  "RC2 (Rating 4) -- loc_2"  = "#4DAF4A",
  "RC3 (Rating 5) -- loc_3"  = "#984EA3"
)

p <- ggplot(plot_data, aes(x = Stage_avg, y = Q_model, colour = rating_label)) +
  geom_ribbon(aes(ymin = Min_CI, ymax = Max_CI, fill = rating_label),
              alpha = 0.15, colour = NA) +
  geom_line(linewidth = 0.8) +
  scale_colour_manual(values = curve_colours) +
  scale_fill_manual(values = curve_colours) +
  theme_bw() +
  xlab("Stage (cm)") + ylab("Discharge (m³/s)") +
  labs(
    title    = "SSN703 -- stage-discharge curves, all ratings",
    subtitle = "Zoom into any overlapping stage range to check how curves relate to each other",
    colour   = "Rating", fill = "Rating"
  ) +
  theme(legend.position = "bottom")

p_interactive <- ggplotly(p, tooltip = c("x", "y", "colour"))


# -----------------------------------------------------------------------------
# 4. Save
# -----------------------------------------------------------------------------

out_path <- file.path(plot_dir, "ssn703_stage_discharge_curves_interactive.html")
saveWidget(p_interactive, file = normalizePath(out_path, mustWork = FALSE), selfcontained = TRUE)

message("\nSaved: ", out_path)
message("Zoom/pan with plotly's toolbar or by dragging a box on the plot.")
message("Double-click to reset zoom. Click a legend entry to hide/show that curve.")
message("Check the console overlap ranges above -- zoom directly into those")
message("stage windows to see whether the curves agree, diverge smoothly, or")
message("show an unexpected kink right at the boundary.")