# =============================================================================
# sn_discovery.R -- nimble sn/views lookup, no memorized site/view/component
# strings required
# =============================================================================
# Source stage_qc/stage_qc_functions.R FIRST (unmodified, reused as-is for
# sn_connect / sn_read_values / sn_read_qc / sn_qc_table_name / sn_read_station
# -- those are already generic, nothing to duplicate here).
#
# Function naming follows the same convention as stage_qc_functions.R /
# qc-functions.R: hydrocan/tidyhydat-style <prefix>_<verb>_<noun>(),
# snake_case, tibble in -> tibble out. `sn_` = Hakai Sensor Network API access.
#
# Sections:
#   A. sn_list_views()      / sn_find_views()      -- what sites/views exist
#   B. sn_list_components() / sn_find_components()  -- what's inside one view
#   C. sn_read_measurement()                        -- download a component's
#                                                       full Avg/Min/Max/Std
#                                                       family in one call
# =============================================================================

library(tidyverse)
library(glue)


# #############################################################################
# A. Views -- what site:view combinations exist at all
# #############################################################################
#' List every site:view combination the Hakai Sensor Network API knows about
#'
#' Wraps `client$get("api/sn/views")` (the bare views endpoint, no
#' site:view specified -- confirmed working the same way `api/sn/tables/list`
#' lists every QC table). Splits the single `value` column (e.g.
#' `"Hecate:5minuteSamples"`) into `site`/`view` columns so you can filter on
#' either without string-parsing yourself.
#'
#' @param client A client from [sn_connect()].
#' @return A tibble: `endpoint` (raw `"site:view"` string), `site`, `view`.
#' @export
sn_list_views <- function(client) {
  # NOTE: the bare "api/sn/views" endpoint this docstring used to claim
  # ("confirmed working the same way api/sn/tables/list lists every QC
  # table") 404s -- it was never actually exercised against the live API.
  # "api/sn/views/list" (mirroring the tables/list naming) is the real one,
  # confirmed 2026-09-24: returns all ~1386 site:view rows in one call, no
  # pagination needed.
  result <- tryCatch(
    client$get("api/sn/views/list"),
    error = function(e) {
      message(glue("Couldn't list views: {conditionMessage(e)}"))
      NULL
    }
  )

  if (is.null(result) || !"value" %in% names(result) || nrow(result) == 0) {
    message("No rows returned from api/sn/views/list -- returning an empty tibble rather than erroring.")
    return(tibble(endpoint = character(), site = character(), view = character()))
  }

  result %>%
    rename(endpoint = value) %>%
    separate(endpoint, into = c("site", "view"), sep = ":", remove = FALSE, extra = "merge") %>%
    arrange(site, view)
}


#' Search for site:view combinations by a site-name pattern
#'
#' So you never have to know in advance whether a site is called `"Hecate"`,
#' `"SA_Hecate"`, or something else entirely -- search instead of memorize.
#'
#' @param client A client from [sn_connect()].
#' @param pattern Regex (or plain substring) to match against `site`.
#' @param ignore_case Case-insensitive match? Default `TRUE`.
#' @return A tibble, same shape as [sn_list_views()], filtered to matches.
#'   0 rows (with a message, not an error) if nothing matches.
#' @export
sn_find_views <- function(client, pattern, ignore_case = TRUE) {
  views <- sn_list_views(client)

  matches <- views %>%
    filter(str_detect(site, regex(pattern, ignore_case = ignore_case)))

  if (nrow(matches) == 0) {
    message(glue("No site:view combinations matched '{pattern}'. ",
                 "Known sites: {paste(unique(views$site), collapse = ', ')}"))
  }

  matches
}


#' Resolve a guessed site string against the real site list
#'
#' [sn_list_components()] (and therefore [sn_find_components()] and
#' [sn_read_measurement()]) need an EXACT site string -- they probe
#' `api/sn/views/{site}:{view}` directly. A near-miss guess like `"SSN844"`
#' when the real site is `"SSN844US"` doesn't fail cleanly: the API 500s on
#' the bad path, which looks like a server error rather than "wrong site
#' name". This reuses [sn_find_views()]'s substring match to turn that into
#' an actionable resolution or message instead.
#'
#' @param client A client from [sn_connect()].
#' @param site The site string as typed/guessed.
#' @return If `site` matches exactly one known site as a substring
#'   (case-insensitive), that match (with a message noting the swap). If
#'   zero or multiple candidates match, `site` unchanged -- the caller's own
#'   probe/error handling still applies -- but with a message listing
#'   whatever candidates (if any) were found.
#' @export
sn_resolve_site <- function(client, site) {
  known <- unique(sn_list_views(client)$site)
  candidates <- known[str_detect(known, regex(site, ignore_case = TRUE))]

  if (length(candidates) == 1) {
    message(glue("Site '{site}' not found -- using closest match '{candidates}'. ",
                 "Pass the exact name to skip this lookup next time."))
    return(candidates)
  }

  if (length(candidates) > 1) {
    message(glue("Site '{site}' not found -- {length(candidates)} possible matches: ",
                 "{paste(candidates, collapse = ', ')}. Use one of these explicitly."))
  } else {
    message(glue("Site '{site}' not found and nothing close matched. Known sites: ",
                 "{paste(known, collapse = ', ')}"))
  }

  site
}


# #############################################################################
# B. Components -- what's inside one site:view
# #############################################################################
#' List every component/field available inside one site:view
#'
#' Probes the view itself with `limit=1` and no `fields=` filter, then reads
#' back whatever column names the API returns -- this is the only way to see
#' a view's actual components; [sn_list_views()] only enumerates site:view
#' combinations, not what's inside them.
#'
#' @param client A client from [sn_connect()].
#' @param site,view Site code and view name, e.g. from [sn_find_views()].
#' @return A tibble: `field` (raw `"site:component"` string), `site`,
#'   `component`. 0 rows (with a message) if the probe comes back empty.
#'   If the direct probe errors (e.g. a bad site name), retries once
#'   against the closest site match from [sn_resolve_site()] before giving
#'   up -- see that function for how the match is chosen.
#' @export
sn_list_components <- function(client, site, view) {
  probe_view <- function(s) {
    tryCatch(
      client$get(glue("api/sn/views/{s}:{view}?limit=1")),
      error = function(e) {
        message(glue("Couldn't probe {s}:{view}: {conditionMessage(e)}"))
        NULL
      }
    )
  }

  probe <- probe_view(site)

  if (is.null(probe)) {
    resolved <- sn_resolve_site(client, site)
    if (!identical(resolved, site)) {
      site <- resolved
      probe <- probe_view(site)
    }
  }

  if (is.null(probe) || length(names(probe)) == 0) {
    return(tibble(field = character(), site = character(), component = character()))
  }

  fields <- setdiff(names(probe), "measurementTime")

  if (length(fields) == 0) {
    message(glue("{site}:{view} returned only measurementTime -- no components found."))
    return(tibble(field = character(), site = character(), component = character()))
  }

  tibble(field = fields) %>%
    separate(field, into = c("site", "component"), sep = ":", remove = FALSE, extra = "merge")
}


#' Search for a component by a name pattern, within one site:view
#'
#' So you never have to know in advance whether relative humidity is called
#' `"RH"`, `"RH_Avg"`, or `"RelHumidity"` -- search instead of memorize.
#'
#' @param client A client from [sn_connect()].
#' @param site,view As in [sn_list_components()].
#' @param pattern Regex (or plain substring) to match against `component`.
#' @param ignore_case Case-insensitive match? Default `TRUE`.
#' @return A tibble, same shape as [sn_list_components()], filtered to
#'   matches. 0 rows (with a message) if nothing matches.
#' @export
sn_find_components <- function(client, site, view, pattern, ignore_case = TRUE) {
  components <- sn_list_components(client, site, view)

  matches <- components %>%
    filter(str_detect(component, regex(pattern, ignore_case = ignore_case)))

  if (nrow(matches) == 0) {
    message(glue("No components in {site}:{view} matched '{pattern}'. ",
                 "Available: {paste(components$component, collapse = ', ')}"))
  }

  matches
}


# #############################################################################
# C. sn_read_measurement() -- download a component's full stat family
# #############################################################################
#' Download a component's value AND its Avg/Min/Max/Std burst statistics
#'
#' The DB upload schema needs quality level, flag, UNESCO Q-level,
#' measurement average, min, max, and std -- not just a single value column.
#' Sensor network views commonly expose burst statistics as separate
#' components sharing one base name (e.g. `RH_Avg`, `RH_Min`, `RH_Max`,
#' `RH_Std`, or sometimes just a bare `RH` standing in for the average).
#' This checks (via [sn_list_components()]) which of those actually exist
#' for this site:view, pulls whichever are present in one [sn_read_values()]
#' call, and reshapes to one row per timestamp with all four stat columns --
#' so every download carries them automatically, rather than requiring you
#' to remember to ask for `_Min`/`_Max`/`_Std` on top of the plain value.
#'
#' @param client A client from [sn_connect()].
#' @param site,view Site code and view name.
#' @param base_component A component name, with or without an `_Avg`/`_Min`/
#'   `_Max`/`_Std` suffix already on it (either is fine -- the suffix, if
#'   any, is stripped to get the base name before searching for the full
#'   family). Typically whatever [sn_find_components()] resolved.
#' @param start_date,end_date Date range as `"YYYY-MM-DD"` strings.
#' @return A tibble: `measurement_time`, `value` (the average -- from an
#'   explicit `_Avg` component if one exists, else the bare base-name
#'   component itself), `value_min`, `value_max`, `value_std`. Any stat the
#'   API doesn't expose for this component comes back as `NA` in that
#'   column (not dropped -- the column always exists) with a message noting
#'   which stats were and weren't found.
#' @export
sn_read_measurement <- function(client, site, view, base_component, start_date, end_date) {
  base <- str_remove(base_component, "_(Avg|Min|Max|Std)$")
  candidates <- c(base, paste0(base, c("_Avg", "_Min", "_Max", "_Std")))

  available <- sn_list_components(client, site, view)$component
  components <- intersect(candidates, available)

  empty_result <- tibble(measurement_time = as.POSIXct(character()),
                          value = numeric(), value_min = numeric(),
                          value_max = numeric(), value_std = numeric())

  if (length(components) == 0) {
    message(glue("No components matching base '{base}' (tried: {paste(candidates, collapse = ', ')}) ",
                 "found in {site}:{view} -- returning an empty tibble."))
    return(empty_result)
  }

  message(glue("{site}:{view} '{base}' -- found: {paste(components, collapse = ', ')}",
               if (length(setdiff(candidates, components)) > 0)
                 glue(" (not found: {paste(setdiff(candidates, components), collapse = ', ')})") else ""))

  long <- sn_read_values(client, site, view, components, start_date, end_date)

  if (nrow(long) == 0) return(empty_result)

  primary <- if (paste0(base, "_Avg") %in% components) paste0(base, "_Avg") else base

  wide <- long %>%
    mutate(
      stat = case_when(
        variable == primary       ~ "value",
        str_ends(variable, "_Min") ~ "value_min",
        str_ends(variable, "_Max") ~ "value_max",
        str_ends(variable, "_Std") ~ "value_std",
        TRUE                       ~ NA_character_
      )
    ) %>%
    filter(!is.na(stat)) %>%
    select(measurement_time, stat, value) %>%
    pivot_wider(names_from = stat, values_from = value, values_fn = ~ .x[1])

  for (col in c("value", "value_min", "value_max", "value_std")) {
    if (!col %in% names(wide)) wide[[col]] <- NA_real_
  }

  wide %>%
    select(measurement_time, value, value_min, value_max, value_std) %>%
    arrange(measurement_time)
}
