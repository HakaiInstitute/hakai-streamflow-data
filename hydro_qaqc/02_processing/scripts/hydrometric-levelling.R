# levelling_check.R
# QA for differential levelling surveys entered via DeviceMagic -> Google Sheet.
#
# Sheet layout: one row per instrument setup, repeated under the same submissionid.
# Every setup reads the same points (staff gauge, PTs, benchmarks) from a different
# instrument height, so:
#   * CLOSURE  = do the setups agree on the height differences between points?
#   * MOVEMENT = do those height differences change from one survey to the next?
#
# Rod reading -> elevation:  elev = HI - reading.  Relative to a reference point (ref):
#   rel_elev = reading_ref - reading_point   (positive = point sits above ref)
# HI cancels, so setups can be compared directly.

library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(purrr)

# ---- settings ---------------------------------------------------------------
sheet_url    <- "https://docs.google.com/spreadsheets/d/1Q4Xm9bjasOVpwG7Ppli6XhsIfOvEUh2-89NklYByBNI/edit?gid=1671108033#gid=1671108033"
sheet_tab    <- NULL    # NULL = first tab
closure_tol  <- 0.003   # m; max disagreement between setups within one survey
movement_tol <- 0.003   # m; change between surveys that gets flagged
ref_priority <- c("BM1", "BM2", "BM3", "BM4", "BM5")  # ref = first of these read in every setup at a site
tz           <- "America/Vancouver"
report_dir   <- "site_reports"  # per-site HTML reports (+ index.html) are written here
# Judgement calls recorded outside the script. One row per explained flag:
#   site, point, survey_date (the survey the flag appears on), category, note
# point = a point (PT1), a benchmark pair (BM1-BM3), or "*" for every point at that survey.
# A matching row marks a flagged step / closure failure as "explained", so it no longer
# drives the site status to REVIEW (it stays visible in the report).
annotations_file <- "02_processing/scripts/levelling_annotations.csv"
# Optional station details (site + any extra columns, e.g. station_name, latitude,
# longitude, gauge_datum). Blank cells are skipped in the report.
metadata_file    <- "02_processing/scripts/levelling_site_metadata.csv"

# ---- functions --------------------------------------------------------------
to_time <- function(x) {
  if (inherits(x, "POSIXt")) return(with_tz(x, tz))
  parse_date_time(as.character(x), orders = c("Ymd HMS", "Ymd HM", "Ymd"),
                  tz = tz, quiet = TRUE)
}

tidy_levelling <- function(raw) {
  raw |>
    transmute(
      submissionid,
      survey_time = to_time(Date),
      site = coalesce(Site_Name, New_Site.Site_Name),
      SG  = as.numeric(Survey_measurements.Staff_Gauge),
      PT1 = as.numeric(Survey_measurements.PT_1),
      PT2 = as.numeric(Survey_measurements.PT_2),
      BM1 = as.numeric(Survey_measurements.Benchmark_1),
      BM2 = as.numeric(Survey_measurements.Benchmark_2),
      BM3 = as.numeric(Survey_measurements.Benchmark_3),
      BM4 = as.numeric(Survey_measurements.Benchmark_4),
      BM5 = as.numeric(Survey_measurements.Benchmark_5)
    ) |>
    group_by(submissionid) |>
    mutate(setup = row_number()) |>          # order of rows within the submission
    ungroup() |>
    pivot_longer(SG:BM5, names_to = "point", values_to = "reading") |>
    mutate(reading = na_if(reading, 0)) |>   # 0 = form default for "not measured"
    filter(!is.na(reading))
}

# One row per survey (submission): surveyor's free-text Survey_Comments, technician
# and photo links. The comments column is matched by suffix in case the sheet nests
# it under a group. Setups repeat these fields, so distinct non-blank values are
# collapsed per submission (comments joined with " | ", photos space-separated).
survey_info <- function(raw) {
  ccol <- names(raw)[str_detect(names(raw), regex("Survey_Comments$", ignore_case = TRUE))]
  if (length(ccol) == 0) stop("No 'Survey_Comments' column found in the sheet. Columns: ",
                              paste(names(raw), collapse = ", "))
  col_or_na <- function(nm) if (nm %in% names(raw)) raw[[nm]] else rep(NA_character_, nrow(raw))
  img_cols  <- names(raw)[str_detect(names(raw), "^Attach_Images\\.Image_\\d+$")]
  photos    <- if (length(img_cols))
    apply(as.matrix(raw[img_cols]), 1, function(r) paste(r[!is.na(r) & r != ""], collapse = " "))
  else rep("", nrow(raw))
  uniq <- function(x, sep) paste(unique(x[!is.na(x) & x != ""]), collapse = sep)

  raw |>
    transmute(submissionid,
              survey_time = to_time(Date),
              site        = coalesce(Site_Name, New_Site.Site_Name),
              technician  = str_squish(coalesce(col_or_na("Technician"),
                                                col_or_na("New_Technician.Technician_Name"))),
              comments    = str_squish(.data[[ccol[1]]]),
              photos      = photos) |>
    group_by(submissionid) |>
    summarise(site        = first(site),
              survey_time = first(survey_time),
              technician  = uniq(technician, ", "),
              comments    = uniq(comments, " | "),
              photos      = uniq(unlist(str_split(photos, " ")), " "),
              .groups = "drop")
}

check_levelling <- function(raw, closure_tol = 0.005, movement_tol = 0.005) {

  surveys <- survey_info(raw)
  long <- tidy_levelling(raw)

  # A setup needs >= 2 points to give any height difference
  dropped <- long |>
    group_by(site, survey_time, submissionid, setup) |>
    summarise(n_pts = n(), points_read = paste(point, collapse = ", "), .groups = "drop") |>
    filter(n_pts < 2)
  long <- anti_join(long, dropped, by = c("submissionid", "setup"))

  # One reference point per site, so every survey at a site is on the same footing
  refs <- long |>
    group_by(site) |>
    summarise(
      ref = {
        n_setups <- n_distinct(paste(submissionid, setup))
        in_all   <- names(which(table(point) == n_setups))
        ref_priority[ref_priority %in% in_all][1]
      },
      .groups = "drop"
    )
  if (anyNA(refs$ref)) {
    warning("No benchmark read in every setup at: ",
            paste(refs$site[is.na(refs$ref)], collapse = ", "), " (site skipped)")
    refs <- filter(refs, !is.na(ref))
  }

  rel <- long |>
    inner_join(refs, by = "site") |>
    group_by(submissionid, setup) |>
    mutate(rel_elev = reading[point == ref[1]] - reading) |>
    ungroup()

  # Per survey and point: mean relative elevation, and how far the setups disagree
  survey_elev <- rel |>
    group_by(site, submissionid, survey_time, ref, point) |>
    summarise(n_setups = n(),
              elev_m   = mean(rel_elev),
              spread_m = max(rel_elev) - min(rel_elev),
              .groups = "drop") |>
    mutate(closure = case_when(
      point == ref                  ~ "reference",
      n_setups < 2                  ~ "unchecked (1 setup)",
      round(spread_m, 4) > closure_tol ~ "FAIL",
      TRUE                          ~ "ok"
    ))

  closure <- survey_elev |>
    filter(point != ref) |>
    transmute(site, survey_time, submissionid, ref, point, n_setups,
              spread_mm = round(spread_m * 1000, 1), closure) |>
    arrange(site, survey_time, point)

  closure_summary <- closure |>
    group_by(site, survey_time, submissionid, ref) |>
    summarise(n_setups     = max(n_setups),
              max_spread_mm = max(spread_mm),
              n_fail       = sum(closure == "FAIL"),
              failed_points = paste(point[closure == "FAIL"], collapse = ", "),
              .groups = "drop") |>
    arrange(site, survey_time)

  # Movement of each point relative to the reference
  movement <- survey_elev |>
    arrange(site, point, survey_time) |>
    group_by(site, point) |>
    mutate(prev_survey  = lag(survey_time),
           prev_closure = lag(closure),
           d_prev_mm   = round((elev_m - lag(elev_m)) * 1000, 1),
           d_base_mm   = round((elev_m - first(elev_m)) * 1000, 1),
           moved       = abs(d_prev_mm) > movement_tol * 1000) |>
    ungroup() |>
    filter(point != ref) |>
    select(site, point, ref, survey_time, prev_survey, rel_elev_m = elev_m,
           d_prev_mm, d_base_mm, moved, closure, prev_closure) |>
    arrange(site, point, survey_time)

  # Benchmark-to-benchmark differences: isolates WHICH benchmark moved
  bm <- survey_elev |>
    filter(str_starts(point, "BM")) |>
    select(site, submissionid, survey_time, point, elev_m)
  bm_pairs <- inner_join(bm, bm, by = c("site", "submissionid", "survey_time"),
                         suffix = c("_a", "_b"), relationship = "many-to-many") |>
    filter(point_a < point_b) |>
    transmute(site, survey_time, pair = paste0(point_a, "-", point_b),
              diff_m = elev_m_b - elev_m_a) |>
    arrange(site, pair, survey_time) |>
    group_by(site, pair) |>
    mutate(d_prev_mm = round((diff_m - lag(diff_m)) * 1000, 1),
           d_base_mm = round((diff_m - first(diff_m)) * 1000, 1),
           moved     = abs(d_prev_mm) > movement_tol * 1000) |>
    ungroup()

  list(closure = closure, closure_summary = closure_summary,
       movement = movement, bm_pairs = bm_pairs, dropped_setups = dropped,
       surveys = surveys, refs = refs, elevations = survey_elev,
       tol = c(closure_mm = closure_tol * 1000, movement_mm = movement_tol * 1000))
}

# ---- annotations & metadata -------------------------------------------------

read_annotations <- function(path) {
  empty <- tibble(site = character(), point = character(), survey_date = as.Date(character()),
                  category = character(), note = character())
  if (is.null(path) || !file.exists(path)) return(empty)
  a <- as_tibble(read.csv(path, colClasses = "character", stringsAsFactors = FALSE))
  need <- c("site", "point", "survey_date", "category", "note")
  if (!all(need %in% names(a))) stop("Annotations file needs columns: ", paste(need, collapse = ", "))
  a |>
    transmute(site = trimws(site), point = trimws(point),
              survey_date = as.Date(survey_date),
              category = trimws(category), note = trimws(note)) |>
    filter(site != "", !is.na(survey_date))
}

read_metadata <- function(path) {
  if (is.null(path) || !file.exists(path)) return(tibble(site = character()))
  m <- as_tibble(read.csv(path, colClasses = "character", stringsAsFactors = FALSE))
  if (!"site" %in% names(m)) stop("Metadata file needs a 'site' column")
  m
}

# Adds `note` (NA = unexplained) to any table with site, survey_time and a point/pair key
annotate <- function(df, ann, key = "point") {
  df  <- mutate(df, .date = as.Date(survey_time, tz = tz))
  ann <- mutate(ann, note = str_squish(if_else(is.na(category) | category == "", note,
                                               paste0(category, " - ", note))))
  exact <- ann |> filter(point != "*") |>
    group_by(site, point, .date = survey_date) |>
    summarise(note_exact = paste(note, collapse = "; "), .groups = "drop") |>
    rename(!!key := point)
  wild <- ann |> filter(point == "*") |>
    group_by(site, .date = survey_date) |>
    summarise(note_all = paste(note, collapse = "; "), .groups = "drop")
  df |>
    left_join(exact, by = c("site", key, ".date")) |>
    left_join(wild,  by = c("site", ".date")) |>
    mutate(note = coalesce(note_exact, note_all)) |>
    select(-.date, -note_exact, -note_all)
}

apply_annotations <- function(res, ann) {
  res$closure  <- annotate(res$closure,  ann, "point") |>
    mutate(note = if_else(closure == "FAIL", note, NA_character_))  # only failures need explaining
  res$movement <- annotate(res$movement, ann, "point")
  res$bm_pairs <- annotate(res$bm_pairs, ann, "pair")
  res
}

# ---- site reports -----------------------------------------------------------
# One self-contained HTML station report per site (+ index.html), laid out like a
# hydrometric station levelling record: identification, current levelled
# elevations, levelling history, stability assessment, QA detail, method notes.

point_levels <- c("SG", paste0("PT", 1:9), paste0("BM", 1:9))
point_type <- function(p) case_when(p == "SG" ~ "Staff gauge",
                                    str_starts(p, "PT") ~ "Pressure transducer",
                                    str_starts(p, "BM") ~ "Benchmark",
                                    TRUE ~ p)

esc <- function(x) {
  x <- as.character(x); x[is.na(x)] <- ""
  x <- str_replace_all(x, "&", "&amp;")
  x <- str_replace_all(x, "<", "&lt;")
  str_replace_all(x, ">", "&gt;")
}

fmt_date <- function(x) format(x, "%Y-%m-%d", tz = tz)

html_table <- function(df, row_class = NULL, html_cols = character()) {
  if (nrow(df) == 0) return("<p class='muted'>None.</p>")
  cells <- imap(df, function(col, nm)
    if (nm %in% html_cols) replace(as.character(col), is.na(col), "") else esc(col))
  rows <- map_chr(seq_len(nrow(df)), function(i) {
    cls <- if (!is.null(row_class) && !is.na(row_class[i]) && nzchar(row_class[i]))
      sprintf(" class='%s'", row_class[i]) else ""
    paste0("<tr", cls, ">", paste0("<td>", map_chr(cells, i), "</td>", collapse = ""), "</tr>")
  })
  paste0("<div class='scroll'><table><thead><tr>",
         paste0("<th>", esc(names(df)), "</th>", collapse = ""),
         "</tr></thead><tbody>", paste(rows, collapse = ""), "</tbody></table></div>")
}

kv_table <- function(v) {
  paste0("<table class='kv'><tbody>",
         paste0("<tr><th>", esc(names(v)), "</th><td>", esc(v), "</td></tr>", collapse = ""),
         "</tbody></table>")
}

photo_links <- function(x) map_chr(x, function(s) {
  u <- str_split(s, " ")[[1]]; u <- u[str_starts(u, "https?://")]
  if (!length(u)) "" else
    paste(sprintf("<a href='%s' target='_blank' rel='noopener'>%d</a>", esc(u), seq_along(u)), collapse = " ")
})

point_colours <- c("#2a6f97", "#e07a1f", "#3a9d5d", "#b5446e", "#7a5cc7",
                   "#a08a1c", "#4a4a4a", "#1aa3a3")

# Times of everything still needing attention: closure failures and flagged steps
# with no matching annotation
issue_times <- function(cl, mv, bp) {
  c(cl$survey_time[cl$closure == "FAIL" & is.na(cl$note)],
    mv$survey_time[mv$moved %in% TRUE & is.na(mv$note)],
    bp$survey_time[bp$moved %in% TRUE & is.na(bp$note)])
}

site_status <- function(sv, cl, mv, bp) {
  if (nrow(sv) < 2) return("BASELINE ONLY")
  it <- issue_times(cl, mv, bp)
  if (any(it == max(sv$survey_time))) "REVIEW"
  else if (length(it) > 0) "PAST MOVEMENT"
  else "STABLE"
}

status_text <- c("STABLE" = "No unexplained movement or closure failures on record.",
                 "REVIEW" = "Unexplained movement or closure failure in the latest survey - review required.",
                 "PAST MOVEMENT" = "Unexplained movement or closure failure in an earlier survey; the latest survey is clean.",
                 "BASELINE ONLY" = "Baseline survey only - movement cannot yet be assessed.")

# Drift of each point (relative to the site reference) since the first survey
stability_svg <- function(mv, tol_mm) {
  d <- filter(mv, !is.na(d_base_mm))
  if (n_distinct(d$survey_time) < 2)
    return("<p class='muted'>Only one survey so far - no movement history yet.</p>")
  pts <- sort(unique(d$point)); cols <- setNames(rep_len(point_colours, length(pts)), pts)
  W <- 760; H <- 280; ml <- 52; mr <- 16; mt <- 14; mb <- 34
  x  <- as.numeric(d$survey_time); xr <- range(x)
  yr <- range(c(d$d_base_mm, -tol_mm, tol_mm)); yr <- yr + c(-1, 1) * diff(yr) * 0.08
  sx <- function(v) ml + (v - xr[1]) / diff(xr) * (W - ml - mr)
  sy <- function(v) mt + (yr[2] - v) / diff(yr) * (H - mt - mb)
  yt <- pretty(yr, 5); yt <- yt[yt >= yr[1] & yt <= yr[2]]
  xt <- seq(xr[1], xr[2], length.out = 5)
  lines <- map_chr(pts, function(p) {
    dp <- filter(d, point == p) |> arrange(survey_time)
    xs <- sx(as.numeric(dp$survey_time)); ys <- sy(dp$d_base_mm)
    mvd  <- dp$moved %in% TRUE
    expl <- mvd & !is.na(dp$note)
    ring <- ifelse(!mvd, cols[[p]], ifelse(expl, "#c98a00", "#d62728"))
    sprintf("<polyline fill='none' stroke='%s' stroke-width='1.8' points='%s'/>%s", cols[[p]],
            paste(round(xs, 1), round(ys, 1), sep = ",", collapse = " "),
            paste(sprintf("<circle cx='%.1f' cy='%.1f' r='%s' fill='%s' stroke='%s' stroke-width='%s'><title>%s %s: %.1f mm from first survey</title></circle>",
                          xs, ys, ifelse(mvd, 5, 3.2), cols[[p]], ring,
                          ifelse(mvd, 2, 1), p, fmt_date(dp$survey_time), dp$d_base_mm), collapse = ""))
  })
  legend <- paste(sprintf("<span><i style='background:%s'></i>%s</span>", cols, pts), collapse = "")
  paste0(
    sprintf("<svg viewBox='0 0 %d %d' role='img' aria-label='Point movement since first survey'>", W, H),
    sprintf("<rect x='%d' y='%.1f' width='%d' height='%.1f' class='band'/>", ml, sy(tol_mm), W - ml - mr, sy(-tol_mm) - sy(tol_mm)),
    paste(sprintf("<line x1='%d' x2='%d' y1='%.1f' y2='%.1f' class='grid'/><text x='%d' y='%.1f' class='tick' text-anchor='end'>%s</text>",
                  ml, W - mr, sy(yt), sy(yt), ml - 6, sy(yt) + 4, yt), collapse = ""),
    paste(sprintf("<text x='%.1f' y='%d' class='tick' text-anchor='middle'>%s</text>",
                  sx(xt), H - 12, fmt_date(as.POSIXct(xt, origin = "1970-01-01", tz = tz))), collapse = ""),
    sprintf("<text transform='translate(12 %.1f) rotate(-90)' class='tick' text-anchor='middle'>mm vs first survey</text>", (H - mb + mt) / 2),
    paste(lines, collapse = ""), "</svg>",
    "<div class='legend'>", legend,
    "<span class='muted'>shaded = movement tolerance; red ring = unexplained step; amber ring = explained step</span></div>")
}

# Plain-language findings for the summary section
stability_summary <- function(sv, cl, mv, bp, ref, tol) {
  li <- function(...) paste0("<li>", paste0(...), "</li>")
  last <- max(sv$survey_time)
  out <- character()
  if (nrow(sv) < 2) {
    out <- li("Only one levelling survey on record (", fmt_date(last), ").")
  } else {
    prev  <- max(sv$survey_time[sv$survey_time < last])
    m_last <- filter(mv, survey_time == last, !is.na(d_prev_mm))
    n_mv  <- sum(m_last$moved %in% TRUE)
    out <- li(sprintf("Latest survey %s vs %s: %d of %d points (excluding reference %s) within %.0f mm of the previous survey.",
                      fmt_date(last), fmt_date(prev), nrow(m_last) - n_mv, nrow(m_last), ref, tol[["movement_mm"]]))
    un <- filter(m_last, moved %in% TRUE, is.na(note))
    ex <- filter(m_last, moved %in% TRUE, !is.na(note))
    if (nrow(un)) out <- c(out, li("<b>Unexplained change:</b> ",
                                   paste(sprintf("%s (%+.0f mm)", esc(un$point), un$d_prev_mm), collapse = ", "), "."))
    if (nrow(ex)) out <- c(out, li("Change attributed to non-movement causes: ",
                                   paste(sprintf("%s (%+.0f mm) - %s", esc(ex$point), ex$d_prev_mm, esc(ex$note)), collapse = "; ")))
    bpu <- filter(bp, survey_time == last, moved %in% TRUE, is.na(note))
    if (nrow(bpu)) out <- c(out, li("<b>Benchmark-to-benchmark change:</b> ", paste(esc(bpu$pair), collapse = ", "), "."))
    n_old <- sum(mv$moved %in% TRUE & is.na(mv$note) & mv$survey_time < last)
    if (n_old) out <- c(out, li(n_old, " unexplained flagged step(s) in earlier surveys."))
  }
  cl_last <- filter(cl, survey_time == last)
  if (nrow(cl_last) == 0) {
    out <- c(out, li("Closure not assessed in the latest survey (fewer than two setups)."))
  } else {
    f <- filter(cl_last, closure == "FAIL")
    out <- c(out, if (nrow(f) == 0)
      li(sprintf("Closure: all points closed within %.0f mm (max setup spread %.1f mm).", tol[["closure_mm"]], max(cl_last$spread_mm)))
    else li("<b>Closure failed:</b> ",
            paste(sprintf("%s (%.1f mm%s)", esc(f$point), f$spread_mm,
                          ifelse(is.na(f$note), "", " - explained")), collapse = ", "), "."))
  }
  paste0("<ul>", paste(out, collapse = ""), "</ul>")
}

site_page <- function(site, res, ann, meta, generated) {
  sv <- filter(res$surveys, site == !!site) |> arrange(survey_time)
  cs <- filter(res$closure_summary, site == !!site)
  cl <- filter(res$closure, site == !!site)
  el <- filter(res$elevations, site == !!site)
  mv <- filter(res$movement, site == !!site)
  bp <- filter(res$bm_pairs, site == !!site)
  dr <- filter(res$dropped_setups, site == !!site)
  an <- filter(ann, site == !!site)
  ref <- res$refs$ref[res$refs$site == site]; ref <- if (length(ref)) ref[1] else NA_character_
  tol <- res$tol
  status <- site_status(sv, cl, mv, bp)
  last <- max(sv$survey_time)
  ord <- function(p) match(p, point_levels)

  # 1. identification
  m <- filter(meta, site == !!site)
  meta_kv <- character()
  if (nrow(m) && ncol(m) > 1) {
    v <- unlist(m[1, setdiff(names(m), "site")]); v <- v[!is.na(v) & nzchar(trimws(v))]
    meta_kv <- setNames(v, str_to_sentence(str_replace_all(names(v), "_", " ")))
  }
  techs <- unique(unlist(str_split(sv$technician, ", "))); techs <- techs[nzchar(techs)]
  pts_all <- unique(el$point); pts_all <- pts_all[order(ord(pts_all))]
  id_kv <- c(`Station ID` = site, meta_kv,
             `Levelling surveys` = sprintf("%d (%s to %s)", nrow(sv), fmt_date(min(sv$survey_time)), fmt_date(last)),
             `Technician(s)` = paste(techs, collapse = ", "),
             `Points levelled` = paste(sprintf("%s (%s)", pts_all, tolower(point_type(pts_all))), collapse = ", "),
             `Reference point` = paste0(ref, " - zero for all relative heights in this report"))
  id_kv <- id_kv[nzchar(id_kv)]

  # 3. latest levelled elevations
  latest <- el |> group_by(point) |> slice_max(survey_time, n = 1, with_ties = FALSE) |> ungroup()
  mv_last <- mv |> group_by(point) |> slice_max(survey_time, n = 1, with_ties = FALSE) |> ungroup() |>
    select(point, d_prev_mm, d_base_mm, moved, note)
  sg_elev <- latest$elev_m[latest$point == "SG"]
  elev <- latest |> left_join(mv_last, by = "point") |> arrange(ord(point))
  elev_tbl <- elev |>
    transmute(Point = point, Type = point_type(point), Surveyed = fmt_date(survey_time),
              `Height rel. to ref. (m)` = sprintf("%.4f", elev_m),
              `Height rel. to SG (m)` = if (length(sg_elev) == 1) sprintf("%.4f", elev_m - sg_elev) else NA_character_,
              `Change vs previous (mm)` = d_prev_mm, `Change vs first (mm)` = d_base_mm,
              `Setup spread (mm)` = ifelse(point == ref, NA, round(spread_m * 1000, 1)),
              Assessment = case_when(point == ref ~ "Reference",
                                     is.na(moved) ~ "Baseline",
                                     moved & !is.na(note) ~ "Changed - explained",
                                     moved ~ "CHANGED",
                                     closure == "FAIL" ~ "Closure failed",
                                     TRUE ~ "Stable"))
  if (length(sg_elev) != 1) elev_tbl <- select(elev_tbl, -`Height rel. to SG (m)`)
  elev_cls <- case_when(elev_tbl$Assessment %in% c("CHANGED", "Closure failed") ~ "flag",
                        elev_tbl$Assessment == "Changed - explained" ~ "explained", TRUE ~ "")

  # 4. history
  pts_read <- el |> group_by(submissionid) |>
    summarise(points = paste(point[order(ord(point))], collapse = ", "), .groups = "drop")
  log <- sv |>
    left_join(select(cs, submissionid, n_setups, max_spread_mm, n_fail, failed_points), by = "submissionid") |>
    left_join(pts_read, by = "submissionid")
  log_tbl <- log |>
    transmute(Date = fmt_date(survey_time), Technician = technician, Setups = n_setups,
              `Points read` = points, `Max spread (mm)` = max_spread_mm,
              Closure = case_when(is.na(n_setups) ~ "not assessed",
                                  n_fail > 0 ~ paste("FAIL:", failed_points), TRUE ~ "ok"),
              Photos = photo_links(photos), Comments = comments)
  log_cls <- ifelse(str_starts(log_tbl$Closure, "FAIL"), "flag", "")
  mat_tbl <- el |> mutate(Date = fmt_date(survey_time), v = sprintf("%.4f", elev_m)) |>
    select(Date, point, v) |>
    pivot_wider(names_from = point, values_from = v, values_fn = function(x) paste(x, collapse = " / ")) |>
    select(Date, any_of(point_levels)) |>
    mutate(across(-Date, ~ coalesce(.x, "-"))) |> arrange(Date)

  # 5. stability
  pt_tbl <- mv |> group_by(Point = point) |>
    summarise(Type = point_type(first(point)), Surveys = n(),
              `Change vs first (mm)` = last(d_base_mm),
              `Max |change| (mm)` = max(abs(d_base_mm), na.rm = TRUE),
              `Steps flagged` = sum(moved %in% TRUE & is.na(note)),
              `Steps explained` = sum(moved %in% TRUE & !is.na(note)),
              `Last flagged` = if (any(moved %in% TRUE)) fmt_date(max(survey_time[moved %in% TRUE])) else "-",
              .groups = "drop") |> arrange(ord(Point))
  bp_tbl <- bp |> group_by(Pair = pair) |>
    summarise(Surveys = n(), `Latest diff (m)` = sprintf("%.4f", last(diff_m)),
              `Change vs first (mm)` = last(d_base_mm),
              `Max |change| (mm)` = max(abs(d_base_mm), na.rm = TRUE),
              `Steps flagged` = sum(moved %in% TRUE & is.na(note)),
              `Steps explained` = sum(moved %in% TRUE & !is.na(note)), .groups = "drop")
  fl <- mv |> filter(moved %in% TRUE) |> arrange(survey_time, ord(point))
  flag_tbl <- fl |>
    transmute(Point = point, `Survey date` = fmt_date(survey_time), `Previous survey` = fmt_date(prev_survey),
              `Step (mm)` = d_prev_mm, `Change vs first (mm)` = d_base_mm,
              `Closure (this / prev.)` = paste(closure, prev_closure, sep = " / "),
              Assessment = ifelse(is.na(note), "Unexplained - review", paste("Explained:", note)))
  flag_cls <- ifelse(is.na(fl$note), "flag", "explained")

  # 6. closure detail
  cl_d <- cl |> arrange(survey_time, ord(point))
  cl_tbl <- cl_d |>
    transmute(Date = fmt_date(survey_time), Point = point, Setups = n_setups,
              `Spread (mm)` = spread_mm, Result = closure, Annotation = note)
  cl_cls <- ifelse(cl_d$closure == "FAIL", ifelse(is.na(cl_d$note), "flag", "explained"), "")
  dr_tbl <- dr |> transmute(Date = fmt_date(survey_time), `Points read` = points_read)
  an_tbl <- an |> arrange(survey_date) |>
    transmute(`Survey date` = format(survey_date), Point = point, Category = category, Note = note)

  sec <- function(n, title, ...) paste0("<section><h2>", n, ". ", title, "</h2>", paste0(..., collapse = ""), "</section>")
  paste0(
    "<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>",
    "<title>", esc(site), " - levelling report</title><style>", report_css, "</style></head><body><main>",
    "<p class='noprint'><a href='index.html'>&larr; all stations</a></p>",
    "<p class='kicker'>Hydrometric station levelling report</p>",
    "<h1>Station ", esc(site), " <span class='badge ", str_replace_all(tolower(status), " ", "-"), "'>", status, "</span></h1>",
    sec(1, "Station identification", kv_table(id_kv)),
    sec(2, "Summary and stability conclusion",
        "<p><b>", esc(status_text[[status]]), "</b></p>",
        stability_summary(sv, cl, mv, bp, ref, tol)),
    sec(3, "Levelled elevations (latest survey of each point)",
        "<p class='muted'>Heights are relative to ", esc(ref), ": positive = above the reference. Change columns: positive = point rose relative to the reference.</p>",
        html_table(elev_tbl, elev_cls)),
    sec(4, "Levelling history",
        "<h3>Survey log</h3>", html_table(log_tbl, log_cls, html_cols = "Photos"),
        "<h3>Height relative to ", esc(ref), " by survey (m)</h3>", html_table(mat_tbl)),
    sec(5, "Stability assessment",
        sprintf("<p class='muted'>Criteria: a point is flagged when its height relative to %s changes by more than %.0f mm between consecutive surveys. Setups within a survey must agree within %.0f mm.</p>",
                esc(ref), tol[["movement_mm"]], tol[["closure_mm"]]),
        stability_svg(mv, tol[["movement_mm"]]),
        "<h3>Movement by point</h3>", html_table(pt_tbl),
        "<h3>Benchmark-to-benchmark</h3>", html_table(bp_tbl),
        "<h3>Flagged steps</h3>", html_table(flag_tbl, flag_cls)),
    sec(6, "Closure detail",
        html_table(cl_tbl, cl_cls),
        if (nrow(dr)) paste0("<h3>Setups dropped (fewer than 2 points read)</h3>", html_table(dr_tbl)) else ""),
    if (nrow(an)) sec(7, "Annotations", "<p class='muted'>Judgements recorded in the annotations file; explained items do not count toward REVIEW.</p>", html_table(an_tbl)) else "",
    sec(if (nrow(an)) 8 else 7, "Method and conventions",
        "<ul>",
        "<li>Differential levelling. Each instrument setup reads the same points; relative height = reference reading minus point reading, so instrument height cancels.</li>",
        "<li>Point height per survey is the mean over setups; the setup spread is the closure check.</li>",
        "<li>Readings of 0 are treated as not measured. Setups with fewer than two points are dropped.</li>",
        "<li>Change vs first is measured from the first survey on record; if the baseline survey was poor, later changes inherit its error.</li>",
        "<li>Survey comments are the technician's field remarks, shown as entered.</li>",
        "</ul>"),
    sec(if (nrow(an)) 9 else 8, "Review",
        sprintf("<p class='muted'>Report generated %s from the levelling survey sheet.</p>", generated),
        "<table class='kv sign'><tbody><tr><th>Reviewed by</th><td></td><th>Date</th><td></td></tr></tbody></table>"),
    "</main></body></html>")
}

report_css <- "
:root{--bg:#fff;--fg:#1c2530;--muted:#6b7683;--line:#dde2e8;--band:#e8f3ea;--flag:#fdecea;--expl:#fdf3dc;--ok:#2e7d4f;--warn:#b26a00;--bad:#c62828;--base:#5a6b80}
@media(prefers-color-scheme:dark){:root{--bg:#14181d;--fg:#e3e8ee;--muted:#93a0ae;--line:#2c343d;--band:#1c3326;--flag:#3a1e1c;--expl:#3a3018;--ok:#5cc489;--warn:#e0a03c;--bad:#ef7a75;--base:#8fa0b5}}
body{background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,sans-serif;margin:0}
main{max-width:900px;margin:0 auto;padding:16px}
.kicker{margin:.4em 0 0;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;font-size:.78rem}
h1{font-size:1.6rem;margin:.1em 0 .2em}h2{margin-top:1.8em;border-bottom:1px solid var(--line)}h3{margin-top:1.4em;font-size:1rem}
a{color:inherit}.muted{color:var(--muted)}.scroll{overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:.9rem}th,td{padding:5px 8px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}
th{color:var(--muted);font-weight:600;white-space:nowrap}tr.flag td{background:var(--flag)}tr.explained td{background:var(--expl)}
table.kv th{width:14em}table.sign td{height:2em;min-width:8em}
.badge{font-size:.75rem;padding:2px 9px;border-radius:99px;border:1px solid currentColor;vertical-align:middle}
.stable{color:var(--ok)}.review{color:var(--bad)}.past-movement{color:var(--warn)}.baseline-only{color:var(--base)}
svg{width:100%;height:auto}.band{fill:var(--band)}.grid{stroke:var(--line)}.tick{fill:var(--muted);font-size:11px}
.legend{display:flex;flex-wrap:wrap;gap:4px 16px;font-size:.85rem}.legend i{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:5px}
@media print{main{max-width:none;padding:0}.noprint{display:none}section{break-inside:avoid-page}tr{break-inside:avoid}a{text-decoration:none}}
"

write_site_reports <- function(res, out_dir = "site_reports", ann = read_annotations(NULL),
                               meta = read_metadata(NULL)) {
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  generated <- format(Sys.time(), "%Y-%m-%d %H:%M", tz = tz)
  sites <- sort(unique(na.omit(res$surveys$site)))
  fname <- function(s) paste0(str_replace_all(s, "[^A-Za-z0-9]+", "_"), ".html")

  idx <- map_dfr(sites, function(s) {
    writeLines(site_page(s, res, ann, meta, generated), file.path(out_dir, fname(s)), useBytes = TRUE)
    sv <- filter(res$surveys, site == s) |> arrange(survey_time)
    cl <- filter(res$closure, site == s); mv <- filter(res$movement, site == s)
    bp <- filter(res$bm_pairs, site == s)
    tibble(site = s, status = site_status(sv, cl, mv, bp), surveys = nrow(sv),
           last_survey = fmt_date(max(sv$survey_time)),
           open_items = sum(issue_times(cl, mv, bp) == max(sv$survey_time)),
           technician = last(sv$technician), last_comment = last(sv$comments))
  })
  rows <- pmap_chr(idx, function(site, status, surveys, last_survey, open_items, technician, last_comment)
    paste0("<tr><td><a href='", fname(site), "'>", esc(site), "</a></td><td>", esc(status), "</td><td>",
           surveys, "</td><td>", last_survey, "</td><td>", open_items, "</td><td>", esc(technician),
           "</td><td>", esc(last_comment), "</td></tr>"))
  writeLines(paste0(
    "<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>",
    "<title>Levelling station reports</title><style>", report_css, "</style></head><body><main>",
    "<h1>Levelling station reports</h1><p class='muted'>Generated ", generated, "</p>",
    "<div class='scroll'><table><thead><tr><th>Station</th><th>Status</th><th>Surveys</th><th>Last survey</th><th>Open items (latest)</th><th>Technician</th><th>Latest comment</th></tr></thead><tbody>",
    paste(rows, collapse = ""), "</tbody></table></div></main></body></html>"),
    file.path(out_dir, "index.html"), useBytes = TRUE)
  invisible(idx)
}

# ---- run --------------------------------------------------------------------
# First time: googlesheets4::gs4_auth() opens a browser and caches the token.
# For scheduled runs: googlesheets4::gs4_auth(path = "service-account.json")
# and share the sheet with the service account's email.
raw <- googlesheets4::read_sheet(sheet_url, sheet = sheet_tab, col_types = "c")  # text, so a stray typo can't make a list-column

ann  <- read_annotations(annotations_file)
meta <- read_metadata(metadata_file)
res  <- apply_annotations(check_levelling(raw, closure_tol, movement_tol), ann)

p <- function(x) print(x, n = Inf, width = Inf)

cat("\n== Setups dropped (fewer than 2 points read) ==\n");  p(res$dropped_setups)
cat("\n== Closure by survey ==\n");                          p(res$closure_summary)
cat("\n== Points failing closure ==\n");                     p(filter(res$closure, closure == "FAIL"))
cat("\n== Movement flagged (vs previous survey) ==\n");      p(filter(res$movement, moved))
cat("\n== Benchmark-pair changes flagged ==\n");             p(filter(res$bm_pairs, moved))

idx <- write_site_reports(res, report_dir, ann, meta)
cat("\n== Site reports written to", normalizePath(report_dir), "==\n"); p(idx)
