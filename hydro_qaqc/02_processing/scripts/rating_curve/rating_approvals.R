# =============================================================================
# rating_approvals.R -- maintain the SSN703 per-gauging rating-curve approval file
# =============================================================================
#   03_docs/metadata/ssn703_rating_approvals.csv
#
# This is the MANUAL accept / reject flag used for curve fitting, for ALL THREE
# ratings (RC1 / RC2 / RC3). Emily edits `final_rating` (Y/N) in that file after
# reviewing the gaugings in fit_rating_curves.Rmd's "Gauging review" plots;
# `reviewed` (Y/N) and `review_note` are hers to annotate. Every OTHER column is
# a refreshed convenience copy of the gauging table -- edits to those are
# discarded on the next sync.
#
# sync_rating_approvals(gaugings, meta_dir):
#   - reconciles the approval file against the current gauging table
#     (adds new gaugings, drops stale rows, refreshes the copied columns,
#      NEVER changes final_rating / reviewed / review_note for a known gauging)
#   - keeps a dated backup of the previous file
#   - returns `gaugings` with:
#       Final_rating_curve      <- the approval-resolved Y/N  (overwritten)
#       Final_rating_curve_mk   <- MK's original raw value    (preserved)
#       final_rating_basis      <- how the seed value was chosen
#       final_rating_reviewed   <- the `reviewed` flag
#
# Seed rule (used only for a gauging not yet in the file):
#   RC1  -> MK's raw Final_rating_curve, verbatim (her published-curve curation)
#   RC2  -> "Y" if stage_status ok and WY != 2020-2021, else "N"
#   RC3  -> "Y" if stage_status ok and not the Event-63 ~121 cm rising-limb
#           gauging, else "N"
#   non-ok stage_status (RC2/RC3)         -> "N"
# The seed therefore reproduces exactly the fit set fit_rating_curves.Rmd used
# BEFORE approval gating -- adopting the file changes no curve until it is edited.
# =============================================================================

library(tidyverse)

RATING_APPROVALS_FILE <- "ssn703_rating_approvals.csv"

.mk_gauging_key <- function(EventID, MID) {
  dplyr::case_when(
    !is.na(EventID) ~ sprintf("%.0f", EventID),
    !is.na(MID)     ~ paste0("MID:", sprintf("%.0f", MID)),
    TRUE            ~ NA_character_
  )
}

.chr_id <- function(x) ifelse(is.na(x), NA_character_, sprintf("%.0f", as.numeric(x)))

sync_rating_approvals <- function(gaugings, meta_dir) {
  path <- file.path(meta_dir, RATING_APPROVALS_FILE)

  base <- gaugings |>
    mutate(
      gauging_key           = .mk_gauging_key(EventID, MID),
      Final_rating_curve_mk = dplyr::coalesce(as.character(Final_rating_curve), "N"),
      .e63 = dplyr::coalesce(suppressWarnings(as.integer(Event_no)), -1L) == 63L &
             !is.na(Stage_avg_corrected) &
             Stage_avg_corrected > 120 & Stage_avg_corrected < 122,
      final_rating_basis = case_when(
        rating_curve_period == "RC1"                     ~ "mk_curation",
        stage_status == "stage_missing"                  ~ "stage_missing",
        stage_status == "stage_suspect"                  ~ "stage_suspect",
        rating_curve_period == "RC2" & WY == "2020-2021" ~ "rc2_wy2020_2021_excluded",
        rating_curve_period == "RC2"                     ~ "rc2_fit_default",
        rating_curve_period == "RC3" & .e63              ~ "rc3_event63_excluded",
        rating_curve_period == "RC3"                     ~ "rc3_fit_default",
        TRUE                                             ~ "other"
      ),
      seed_rating = case_when(
        rating_curve_period == "RC1"                     ~ Final_rating_curve_mk,
        stage_status != "ok"                             ~ "N",
        rating_curve_period == "RC2" & WY == "2020-2021" ~ "N",
        rating_curve_period == "RC2"                     ~ "Y",
        rating_curve_period == "RC3" & .e63              ~ "N",
        rating_curve_period == "RC3"                     ~ "Y",
        TRUE                                             ~ "N"
      ),
      seed_rating = if_else(seed_rating %in% c("Y", "N"), seed_rating, "N")
    )

  if (any(is.na(base$gauging_key)))
    stop("sync_rating_approvals: ", sum(is.na(base$gauging_key)),
         " gauging(s) have neither EventID nor MID -- cannot key the approval file")
  if (any(duplicated(base$gauging_key)))
    stop("sync_rating_approvals: duplicate gauging_key(s): ",
         paste(unique(base$gauging_key[duplicated(base$gauging_key)]), collapse = ", "))

  display <- base |>
    transmute(
      gauging_key,
      EventID = .chr_id(EventID), MID = .chr_id(MID), Event_no,
      Date, datetime, WY, rating_curve_period, Method, stage_status,
      stage_source,
      Stage_avg_corrected = round(Stage_avg_corrected, 1),
      # cm change vs. the pre-reassignment stage; only meaningful where
      # stage_source == "pls4_reassigned" -- see
      # 03_docs/decisions/ssn703_pls3_pls4_rc_boundary.md
      stage_change_cm = round(Stage_avg_corrected - Stage_avg_corrected_old, 1),
      Q_meas = round(Q_meas, 4), Q_rel_unc,
      Comments, final_rating_basis, seed_rating
    ) |>
    arrange(datetime)

  prev <- if (file.exists(path)) {
    readr::read_csv(path, show_col_types = FALSE,
                    col_types = cols(.default = col_character())) |>
      select(gauging_key, final_rating, reviewed, review_note) |>
      distinct(gauging_key, .keep_all = TRUE)
  } else {
    tibble(gauging_key = character(), final_rating = character(),
           reviewed = character(), review_note = character())
  }

  merged <- display |>
    left_join(prev, by = "gauging_key") |>
    mutate(
      final_rating = toupper(trimws(dplyr::coalesce(final_rating, seed_rating))),
      final_rating = if_else(final_rating %in% c("Y", "N"), final_rating, seed_rating),
      reviewed     = toupper(trimws(dplyr::coalesce(reviewed, "N"))),
      reviewed     = if_else(reviewed %in% c("Y", "N"), reviewed, "N")
    ) |>
    relocate(final_rating, reviewed, review_note, .after = last_col())

  # ---- report -------------------------------------------------------------
  n_new   <- sum(!merged$gauging_key %in% prev$gauging_key)
  orphans <- setdiff(prev$gauging_key, merged$gauging_key)

  message("\n-- rating approvals (", RATING_APPROVALS_FILE, ") --")
  message("  ", nrow(merged), " gaugings; ", n_new, " added this sync",
          if (length(orphans))
            paste0("; ", length(orphans), " stale row(s) dropped: ",
                   paste(orphans, collapse = ", ")) else "")
  merged |>
    count(rating_curve_period, final_rating) |>
    tidyr::pivot_wider(names_from = final_rating, values_from = n, values_fill = 0) |>
    as.data.frame() |> print(row.names = FALSE)
  unrev <- merged |> filter(reviewed != "Y", final_rating == "Y") |>
    count(rating_curve_period, name = "approved_unreviewed")
  if (nrow(unrev)) {
    message("  approved but not yet marked reviewed:")
    print(as.data.frame(unrev), row.names = FALSE)
  }

  if (file.exists(path))
    file.copy(path,
              file.path(meta_dir, paste0("ssn703_rating_approvals_prev_", Sys.Date(), ".csv")),
              overwrite = TRUE)
  readr::write_csv(merged, path, na = "")
  message("  saved: ", path)

  # ---- return the gauging table with the resolved flag -------------------
  base |>
    select(-Final_rating_curve, -.e63, -seed_rating) |>
    left_join(merged |> select(gauging_key, Final_rating_curve = final_rating,
                               final_rating_reviewed = reviewed),
              by = "gauging_key") |>
    select(-gauging_key)
}
