library(readr)
library(dplyr)
library(lubridate)
library(plotly)

# ---- Load the auto-flagged file from step 2 ----
ssn703_d_flagged <- read_csv(
  "C:/Users/Emily/Documents/git-repos/hydro_qaqc/01_raw/SSN703/ssn703_d_rc5_autoflagged.csv",
  col_types = cols(
    measurement_time = col_datetime(),  # auto-parses ISO8601 T...Z as UTC
    .default = col_guess()
  )
) %>%
  mutate(measurement_time = with_tz(measurement_time, "Etc/GMT+8"))

# ---- Build the interactive plot ----
# scattergl (WebGL) used throughout since this is ~475k points - regular
# scatter/line traces will be very sluggish to pan/zoom at this size.

p <- plot_ly() %>%
  add_trace(
    data = ssn703_d_flagged,
    x = ~measurement_time, y = ~stage_avg,
    type = "scattergl", mode = "lines",
    line = list(color = "steelblue", width = 1),
    name = "stage_avg"
  ) %>%
  add_trace(
    data = ssn703_d_flagged,
    x = ~measurement_time, y = ~baseline,
    type = "scattergl", mode = "lines",
    line = list(color = "gray", width = 1, dash = "dash"),
    name = "rolling baseline"
  )

# ---- Overlay each flag type as its own colored marker trace ----
flag_colors <- c(
  RANGE    = "red",
  SPIKE    = "orange",
  FLATLINE = "purple",
  GAP      = "black",
  SHIFT    = "green"
)

for (flag_type in names(flag_colors)) {
  flagged_subset <- ssn703_d_flagged %>% filter(flag_auto == flag_type)
  if (nrow(flagged_subset) > 0) {
    p <- p %>% add_trace(
      data = flagged_subset,
      x = ~measurement_time, y = ~stage_avg,
      type = "scattergl", mode = "markers",
      marker = list(color = flag_colors[[flag_type]], size = 5),
      name = flag_type
    )
  }
}

p <- p %>% layout(
  title = "SSN703 PT4 (RC5) - Automated QC Review",
  xaxis = list(title = "", rangeslider = list(visible = TRUE)),
  yaxis = list(title = "Stage (m)"),
  legend = list(orientation = "h")
)

p

# ---- Optionally save as standalone HTML to open in a browser ----
htmlwidgets::saveWidget(
  p,
  "C:/Users/Emily/Documents/git-repos/hydro_qaqc/01_raw/SSN703/ssn703_d_qc_review.html",
  selfcontained = TRUE
)