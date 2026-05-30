# =============================================================================
# 08b_extend_rating2_lookup_844.R
# Extend SSN844 Rating 2 lookup table from 146cm to 160cm
#
# Approach:
#   - Keep existing Rating 2 lookup as-is up to 146cm
#   - Use power law Q = a(h - h0)^b fitted to existing lookup to extrapolate
#     new rows from 146.1cm to 160cm
#   - CI for extrapolated rows uses same proportional approach as existing lookup
#   - Bind new rows onto existing lookup and save
#
# Fitted coefficients (from nlsLM on full Rating 2 lookup):
#   a  = 2.737e-04
#   b  = 2.279
#   h0 = 31.40 cm
#
# Note: extrapolated rows (>146cm) are beyond the gauged range and should
#       be flagged as such in any output metadata.
# =============================================================================

library(tidyverse)
library(minpack.lm)

# -----------------------------------------------------------------------------
# 1. Read existing lookup, filter to Rating 2
# -----------------------------------------------------------------------------

lookup_prev <- read_csv("03_docs/metadata/ssn844_RC1_lookup_v_previous.csv")

r2 <- lookup_prev |>
  filter(Rating == 2) |>
  arrange(Stage_avg)

cat("Existing Rating 2 rows:", nrow(r2), "\n")
cat("Stage range:", min(r2$Stage_avg), "to", max(r2$Stage_avg), "cm\n")

# -----------------------------------------------------------------------------
# 2. Fit power law to recover coefficients (used only for extrapolation)
# -----------------------------------------------------------------------------

nls_fit <- nlsLM(
  Q_model ~ a * (Stage_avg - h0)^b,
  data = r2,
  start = list(a = 0.01, b = 2, h0 = 28),
  lower = c(a = 0, b = 1, h0 = 0),
  upper = c(a = 10, b = 5, h0 = min(r2$Stage_avg) - 0.1)
)

coefs <- coef(nls_fit)
a  <- coefs["a"]
b  <- coefs["b"]
h0 <- coefs["h0"]

cat("\nFitted coefficients (for extrapolation only):\n")
cat("  a  =", round(a, 6), "\n")
cat("  b  =", round(b, 4), "\n")
cat("  h0 =", round(h0, 4), "cm\n")

# -----------------------------------------------------------------------------
# 3. Estimate CI proportions from existing lookup for extrapolation
#    Use upper portion of existing curve (>100cm) to estimate CI at high stage
# -----------------------------------------------------------------------------

r2_ci <- r2 |>
  filter(Q_model > 0.01) |>
  mutate(
    max_offset_prop = (Max_CI - Q_model) / Q_model,
    min_offset_prop = (Q_model - Min_CI) / Q_model
  )

# Fit linear models of CI offset proportion vs stage
ci_max_fit <- lm(max_offset_prop ~ Stage_avg, data = r2_ci)
ci_min_fit <- lm(min_offset_prop ~ Stage_avg, data = r2_ci)

# -----------------------------------------------------------------------------
# 4. Build extrapolated rows from 146.1cm to 160cm
# -----------------------------------------------------------------------------

stage_new <- seq(146.1, 160, by = 0.1) |> round(1)

q_new <- a * (stage_new - h0)^b

# Predict CI proportions and compute CI bounds
max_prop_new <- predict(ci_max_fit, newdata = data.frame(Stage_avg = stage_new))
min_prop_new <- predict(ci_min_fit, newdata = data.frame(Stage_avg = stage_new))

max_ci_new <- q_new * (1 + max_prop_new)
min_ci_new <- pmax(q_new * (1 - min_prop_new), 0)

lookup_new <- tibble(
  Start     = as.POSIXct("2017-07-13 00:00:00"),
  Stage_avg = stage_new,
  Q_model   = round(q_new, 4),
  Max_CI    = round(max_ci_new, 4),
  Min_CI    = round(min_ci_new, 4),
  Rating    = 2
)

cat("\nExtrapolated rows:", nrow(lookup_new), "\n")
cat("Q at 160cm:", round(max(q_new), 3), "m³/s\n")

# -----------------------------------------------------------------------------
# 5. Bind and save
# -----------------------------------------------------------------------------

lookup_extended <- bind_rows(r2, lookup_new) |>
  arrange(Stage_avg)

# Sanity check plot
p_ext <- ggplot(lookup_extended, aes(x = Stage_avg)) +
  geom_ribbon(aes(ymin = Min_CI, ymax = Max_CI), fill = "grey70", alpha = 0.5) +
  geom_line(aes(y = Q_model), colour = "black", linewidth = 0.8) +
  geom_vline(xintercept = 146, colour = "red", linetype = "dashed") +
  annotate(
    "text", x = 147, y = max(lookup_extended$Q_model) * 0.5,
    label = "Extrapolated\nabove 146cm",
    hjust = 0, size = 3, colour = "red"
  ) +
  labs(
    title = "SSN844 Rating 2 extended to 160cm",
    subtitle = paste0(
      "Extrapolation: Q = ", round(a, 6),
      " * (h - ", round(h0, 3), ")^", round(b, 4),
      "  [above 146cm only]"
    ),
    x = "Stage (cm)", y = "Discharge (m³/s)"
  ) +
  theme_bw()

print(p_ext)

write_csv(lookup_extended, "04_outputs/ssn844_RC2_rating_curve_v2.csv")

cat("\nDone. Extended lookup saved to 04_outputs/ssn844_RC2_rating_curve_v2.csv\n")
cat("Total rows:", nrow(lookup_extended), "\n")
cat("Stage range:", min(lookup_extended$Stage_avg), "to", max(lookup_extended$Stage_avg), "cm\n")
cat("Q range:", round(min(lookup_extended$Q_model), 4), "to", round(max(lookup_extended$Q_model), 3), "m³/s\n")
