# ============================================================
# Download & prep SSN703 PLS (level + temperature) data,
# with QC flags, from the Hakai Sensor Network API
# ============================================================

library(tidyverse)
library(glue)
library(hakaiApi)

# ---- 1. Connect to the Hakai API -------------------------------------------
client <- hakaiApi::Client$new("https://portal.hakai.org")

# ---- 2. Define what to pull -------------------------------------------------
site       <- "SSN703US"          # swap for "SSN703DS" / "SSN703" etc. as needed
view       <- "5minuteSamples"
components <- c("PLS_Lvl", "PLS_Temp")

start_date <- "2026-07-31"
end_date   <- "2026-08-02"

# ---- 3. Pull the value columns -------------------------------------------------
# Confirmed working: SSN703US:PLS_Lvl and SSN703US:PLS_Temp on the sn/views
# endpoint. No QC columns live on this view -- those come from a separate
# endpoint (step 4).
field_names <- paste(site, components, sep = ":")
fields <- paste(c("measurementTime", field_names), collapse = ",")

values_query <- glue(
  "api/sn/views/{site}:{view}?fields={fields}",
  "&measurementTime>{start_date}",
  "&measurementTime<{end_date}",
  "&limit=-1"
)

sn_data <- client$get(values_query)

sn_long <- sn_data %>%
  rename(date = measurementTime) %>%
  pivot_longer(
    cols = -date,
    names_to = c("site", "variable"),
    names_sep = ":",
    values_to = "value"
  ) %>%
  arrange(variable, date)

glimpse(sn_long)

# ---- 3b. List every sn table the API knows about, just to look ------------------
# A "table" here is just a named spreadsheet on Hakai's server. We've already
# checked the one table tied to SSN703's 5-minute samples (ssn703us_5minute)
# thoroughly -- its full QC history has no PLS_Lvl entries. This just lists
# every table name that exists, in case level QC lives somewhere under a name
# we haven't guessed. No expertise needed -- just scan the printed list for
# anything that looks related to SSN703 or level/lvl.
all_tables <- client$get("api/sn/tables/list")
print(all_tables, n = Inf)
# REPORT BACK: anything in this list that looks related to SSN703 level data,
# besides ssn703us_5minute?

# ---- 4. Pull QC flags from the separate qc endpoint -----------------------------
# Confirmed: the real underlying table name is snake_case, NOT the view-style
# name -- "ssn703us_5minute". Confirmed via api/sn/qc/:tableName (documented at
# https://hakaiinstitute.github.io/hakai-api/endpoints/).
#
# Per the sensor metadata sheet, PLS_Lvl (Primary/raw) and Stage (Calculated,
# derived from PLS_Lvl) are genuinely separate measurements. The QC table's full
# measurement_name history is only: "Stage", "PLS_Temp", "PLS2_Temp",
# "PLS3_Temp", "Turbidity" -- there is currently no manual QC data for the raw
# PLS_Lvl channel itself, only for its calculated derivative Stage, which is
# deliberately excluded here. So: QC flags get merged for PLS_Temp only.
table_name <- "ssn703us_5minute"

qc_query <- glue(
  "api/sn/qc/{table_name}?measurement_time>{start_date}",
  "&measurement_time<{end_date}",
  "&limit=-1"
)

qc_flags <- client$get(qc_query)

qc_flags_clean <- qc_flags %>%
  filter(measurement_name == "PLS_Temp") %>%
  mutate(variable = measurement_name) %>%
  filter(
    measurement_time >= ymd(start_date),
    measurement_time <  ymd(end_date)
  ) %>%
  select(date = measurement_time, variable, quality_level, qc_flag)

glimpse(qc_flags_clean)

# ---- 5. Merge values with QC flags ----------------------------------------------
# PLS_Lvl rows will have NA quality_level/qc_flag throughout -- expected, since
# no manual QC exists for that channel in this table (see note above), not a
# merge failure.
sn_long <- sn_long %>%
  left_join(qc_flags_clean, by = c("date", "variable"))

glimpse(sn_long)

# ---- 6. Quick look ---------------------------------------------------------------
sn_long %>%
  ggplot(aes(x = date, y = value, color = !is.na(qc_flag))) +
  geom_line(aes(group = variable), color = "grey60") +
  geom_point(data = ~ filter(.x, !is.na(qc_flag)), size = 1) +
  facet_wrap(~variable, scales = "free_y", ncol = 1) +
  labs(title = paste(site, "PLS data,", start_date, "to", end_date),
       subtitle = "QC flags available for PLS_Temp only -- none exist for raw PLS_Lvl",
       x = NULL, y = NULL, color = "Flagged") +
  theme(legend.position = "bottom")

# ---- 7. Export for QC ------------------------------------------------------------
write.csv(
  sn_long,
  glue("{site}_PLS_data_with_qc_{start_date}_to_{end_date}.csv"),
  row.names = FALSE
)