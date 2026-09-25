library(readr)
library(dplyr)

df <- read_csv(
  "04_outputs/ssn703_rating_curve_lookup_combined.csv",
  col_names = c("Start", "Stage", "Discharge", "MaxDischarge", "MinDischarge", "Rating"),
   skip = 1) # add skip = 1 here if the file already has a header row

# --- extend Stage to 218.00 if it stops short (linear, final-interval slope) ---
top <- 218.00; step <- 0.10
n <- round((top - max(df$Stage)) / step)

if (n > 0) {
  last <- tail(df, 1); prev <- tail(df, 2)[1, ]
  i <- seq_len(n)
  ext <- tibble(
    Start        = last$Start,
    Stage        = last$Stage + step * i,
    Discharge    = last$Discharge    + (last$Discharge    - prev$Discharge)    * i,
    MaxDischarge = last$MaxDischarge + (last$MaxDischarge - prev$MaxDischarge) * i,
    MinDischarge = last$MinDischarge + (last$MinDischarge - prev$MinDischarge) * i
  )
  df <- bind_rows(df, ext)
}

# --- format to fixed decimals as text on the way out (kills float noise) ---
df_out <- df |>
  mutate(
    Stage        = sprintf("%.2f", Stage),
    Discharge    = sprintf("%.4f", Discharge),
    MaxDischarge = sprintf("%.4f", MaxDischarge),
    MinDischarge = sprintf("%.4f", MinDischarge)
  )

write_csv(df_out, "04_outputs/ssn703_rating_curve_lookup_combined_clean.csv")
