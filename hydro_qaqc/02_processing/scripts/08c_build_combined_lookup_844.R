# =============================================================================
# SSN844 Rating Curve -- Script 08c: Build Combined Lookup Table
# =============================================================================
# Purpose:
#   Combine Rating 1 (from previous pipeline) and Rating 2 (extended to 160cm)
#   into a single lookup table for use in discharge computation.
#
# Inputs:
#   03_docs/metadata/ssn844_RC1_lookup_v_previous.csv  -- Rating 1 rows
#   04_outputs/ssn844_RC2_rating_curve_v2.csv          -- Rating 2 rows (extended)
#
# Outputs:
#   04_outputs/ssn844_rating_curve_lookup_combined.csv
#
# Notes:
#   - Rating 1 rows are taken as-is from the previous pipeline
#   - Rating 2 rows use the extended lookup (146cm -> 160cm)
#   - Combined table has one row per stage per rating
#   - 10_discharge_844.R filters to the appropriate rating by date
#
# Author: [your name]
# Date: [date]
# =============================================================================

library(tidyverse)

# -----------------------------------------------------------------------------
# 1. Load inputs
# -----------------------------------------------------------------------------

rc1 <- read_csv(
  "03_docs/metadata/ssn844_RC1_lookup_v_previous.csv",
  show_col_types = FALSE
) |>
  filter(Rating == 1)

rc2 <- read_csv(
  "04_outputs/ssn844_RC2_rating_curve_v2.csv",
  show_col_types = FALSE
)

message("Rating 1 rows: ", nrow(rc1))
message("Rating 1 stage range: ", min(rc1$Stage_avg), " to ", max(rc1$Stage_avg), " cm")

message("Rating 2 rows: ", nrow(rc2))
message("Rating 2 stage range: ", min(rc2$Stage_avg), " to ", max(rc2$Stage_avg), " cm")

# -----------------------------------------------------------------------------
# 2. Combine and save
# -----------------------------------------------------------------------------

combined <- bind_rows(rc1, rc2) |>
  arrange(Rating, Stage_avg)

write_csv(combined, "04_outputs/ssn844_rating_curve_lookup_combined.csv")

message("\nSaved: 04_outputs/ssn844_rating_curve_lookup_combined.csv")
message("Total rows: ", nrow(combined))
message("\nRows per rating:")
combined |> count(Rating) |> print()
