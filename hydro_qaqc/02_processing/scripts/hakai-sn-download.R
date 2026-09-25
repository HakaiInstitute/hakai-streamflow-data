# ============================================================
# Reusable Hakai Sensor Network downloader: values + QC flags
# ============================================================
# Usage: edit the STATIONS config list below, then run the script.
# Each entry pulls raw values from sn/views, pulls QC flags from
# sn/qc, and merges them -- for as many sites/variables as you add.

library(tidyverse)
library(glue)
library(hakaiApi)

# ---- Connect ------------------------------------------------------------------
client <- hakaiApi::Client$new("https://portal.hakai.org")

# ================================================================================
# CONFIG -- edit this for each new sensor/variable set you want to pull.
# ================================================================================
# site:       sensor network site code, e.g. "SSN703US"
# view:       sample view name, e.g. "5minuteSamples"
# components: the Site:Component field names to pull raw values for
# qc_name_map: named vector mapping component -> the measurement_name it's
#              actually called under in the QC table. Only needed when they
#              differ (like PLS_Temp matching directly, but a level channel
#              being tracked under a completely different name like "Stage").
#              Leave a component out of this map if you don't know its QC name
#              yet -- the pipeline will tell you what QC names actually exist
#              for that table so you can fill it in.
# start_date / end_date: date range to pull, "YYYY-MM-DD"

STATIONS <- list(
  list(
    site       = "SSN703US",
    view       = "5minuteSamples",
    components = c("PLS_Lvl", "PLS_Temp"),
    qc_name_map = c(PLS_Temp = "PLS_Temp"),  # PLS_Lvl has no QC match -- omitted on purpose
    start_date = "2014-08-02",
    end_date   = "2018-09-12"
  )
  # Add more stations here, e.g.:
  # list(
  #   site       = "SSN626US",
  #   view       = "5minuteSamples",
  #   components = c("PLS_Lvl", "PLS_Temp"),
  #   qc_name_map = c(PLS_Temp = "PLS_Temp"),
  #   start_date = "2026-07-31",
  #   end_date   = "2026-08-02"
  # )
)

# ================================================================================
# FUNCTIONS -- shouldn't need to touch these day to day.
# ================================================================================

# Pull raw values for a site/view/components from sn/views.
pull_values <- function(client, site, view, components, start_date, end_date) {
  field_names <- paste(site, components, sep = ":")
  fields <- paste(c("measurementTime", field_names), collapse = ",")

  query <- glue(
    "api/sn/views/{site}:{view}?fields={fields}",
    "&measurementTime>{start_date}",
    "&measurementTime<{end_date}",
    "&limit=-1"
  )

  client$get(query) %>%
    rename(date = measurementTime) %>%
    pivot_longer(
      cols = -date,
      names_to = c("site", "variable"),
      names_sep = ":",
      values_to = "value"
    ) %>%
    arrange(variable, date)
}

# Derive the real underlying QC table name from a site/view. This is NOT the
# same as the Site:Component naming used in sn/views -- confirmed via testing
# that the qc endpoint wants a snake_case name (e.g. "ssn703us_5minute"), which
# this reproduces: lowercase, strip anything before the last "/", drop
# "...samples" onward, replace ":" with "_".
derive_qc_table_name <- function(site, view) {
  glue("{site}:{view}") %>%
    tolower() %>%
    str_remove(".*/") %>%
    str_remove("samples.*$") %>%
    str_replace_all(":", "_")
}

# Pull QC flags for a table, scoped to a date range. Wrapped in tryCatch since
# a bad table name 500s rather than failing cleanly.
pull_qc_flags <- function(client, table_name, start_date, end_date) {
  query <- glue(
    "api/sn/qc/{table_name}?measurement_time>{start_date}",
    "&measurement_time<{end_date}",
    "&limit=-1"
  )
  tryCatch(
    client$get(query),
    error = function(e) {
      warning(glue("Couldn't reach QC table '{table_name}': {conditionMessage(e)}"))
      tibble(measurement_name = character(), measurement_time = as.POSIXct(character()),
             quality_level = numeric(), qc_flag = character())
    }
  )
}

# Merge QC flags into the long values table, using an explicit component ->
# QC-measurement_name map (since they don't always match). Warns about any
# requested component that has no mapping, and reports what QC names DO exist
# in the table so you can fill the map in for next time.
merge_qc <- function(values_long, qc_flags, qc_name_map, components) {
  unmapped <- setdiff(components, names(qc_name_map))
  if (length(unmapped) > 0) {
    available_names <- if (nrow(qc_flags) > 0) {
      paste(unique(qc_flags$measurement_name), collapse = ", ")
    } else {
      "(no QC data found for this table/date range at all)"
    }
    message(glue(
      "No qc_name_map entry for: {paste(unmapped, collapse = ', ')}. ",
      "These will have NA quality_level/qc_flag. ",
      "measurement_name values that DO exist in this QC table: {available_names}"
    ))
  }

  if (nrow(qc_flags) == 0) {
    return(values_long %>% mutate(quality_level = NA_real_, qc_flag = NA_character_))
  }

  qc_clean <- qc_flags %>%
    filter(measurement_name %in% qc_name_map) %>%
    mutate(variable = names(qc_name_map)[match(measurement_name, qc_name_map)]) %>%
    select(date = measurement_time, variable, quality_level, qc_flag)

  values_long %>% left_join(qc_clean, by = c("date", "variable"))
}

# Run the full pipeline for one station config: pull values, pull QC, merge,
# plot, export. Returns the merged long-format tibble.
run_station <- function(client, cfg) {
  message(glue("--- {cfg$site}:{cfg$view} ---"))

  values_long <- pull_values(client, cfg$site, cfg$view, cfg$components,
                              cfg$start_date, cfg$end_date)

  table_name <- derive_qc_table_name(cfg$site, cfg$view)
  qc_flags <- pull_qc_flags(client, table_name, cfg$start_date, cfg$end_date)

  qc_name_map <- cfg$qc_name_map %||% setNames(cfg$components, cfg$components)
  merged <- merge_qc(values_long, qc_flags, qc_name_map, cfg$components)

  print(
    merged %>%
      ggplot(aes(x = date, y = value, color = !is.na(qc_flag))) +
      geom_line(aes(group = variable), color = "grey60") +
      geom_point(data = ~ filter(.x, !is.na(qc_flag)), size = 1) +
      facet_wrap(~variable, scales = "free_y", ncol = 1) +
      labs(title = glue("{cfg$site} data, {cfg$start_date} to {cfg$end_date}"),
           x = NULL, y = NULL, color = "Flagged") +
      theme(legend.position = "bottom")
  )

  out_file <- glue("{cfg$site}_data_with_qc_{cfg$start_date}_to_{cfg$end_date}.csv")
  write.csv(merged, out_file, row.names = FALSE)
  message(glue("Wrote {out_file}"))

  merged
}

# ================================================================================
# RUN -- pulls every station listed in STATIONS above.
# ================================================================================
results <- map(STATIONS, ~ run_station(client, .x))
names(results) <- map_chr(STATIONS, "site")

# Combined table across all stations, if you want one file with everything:
all_data <- bind_rows(results, .id = "config_site")
