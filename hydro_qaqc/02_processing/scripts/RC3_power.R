library(tidyverse)

# Load prepped gaugings
gaugings_all <- read_csv("03_docs/metadata/ssn703_gaugings_prepped.csv") |>
  filter(!is.na(Q_meas)) |>
  mutate(
    Stage_avg = Stage_avg_corrected,
    Q_abs_unc = Q_rel_unc / 100 * Q_meas
  )

# RC3 gaugings -- same filters as 08
HQ_RC3 <- gaugings_all |>
  filter(rating_curve_period == "RC3",
         stage_status == "ok",
         Old_New == "New",
         Final_rating_curve == "Y") |>
  filter(!(Event_no == 63 & Stage_avg > 120 & Stage_avg < 122))

# h0
h0_RC3_pl <- min(HQ_RC3$Stage_avg[HQ_RC3$Q_meas > 0], na.rm = TRUE) * 0.9

# Log-log starting values
RC3_pl_loglog <- HQ_RC3 |>
  filter(Stage_avg > h0_RC3_pl, Q_meas > 0) |>
  mutate(log_h = log(Stage_avg - h0_RC3_pl), log_Q = log(Q_meas))

lm_pl_RC3  <- lm(log_Q ~ log_h, data = RC3_pl_loglog)
a_pl_start <- exp(coef(lm_pl_RC3)[1])
b_pl_start <- coef(lm_pl_RC3)[2]

# Fit power law on all data
fit_RC3_pl <- tryCatch(
  nls(Q_meas ~ a * (Stage_avg - h0)^b,
      data    = HQ_RC3 |> filter(Stage_avg > h0_RC3_pl, Q_meas > 0),
      start   = list(a = a_pl_start, b = b_pl_start, h0 = h0_RC3_pl),
      control = nls.control(maxiter = 500)),
  error = function(e) {
    message("Falling back to fixed h0")
    nls(Q_meas ~ a * (Stage_avg - h0_RC3_pl)^b,
        data    = HQ_RC3 |> filter(Stage_avg > h0_RC3_pl, Q_meas > 0),
        start   = list(a = a_pl_start, b = b_pl_start),
        control = nls.control(maxiter = 500))
  }
)

RC3_pl_params <- coef(fit_RC3_pl)
message("Power law: a = ", round(RC3_pl_params["a"], 4),
        ", b = ", round(RC3_pl_params["b"], 4),
        ", h0 = ", round(RC3_pl_params["h0"], 1))

# Generate curve
RC3_pl_stage <- seq(min(HQ_RC3$Stage_avg), 218, by = 0.1)
RC3_pl_Q     <- predict(fit_RC3_pl, newdata = data.frame(Stage_avg = RC3_pl_stage))
RC3_pl_curve <- tibble(Stage_avg = RC3_pl_stage, Q_model = RC3_pl_Q)

# Plot
ggplot() +
  geom_pointrange(data = HQ_RC3,
                  aes(x = Stage_avg, y = Q_meas,
                      ymin = Q_meas - Q_abs_unc,
                      ymax = Q_meas + Q_abs_unc)) +
  geom_line(data = RC3_pl_curve,
            aes(x = Stage_avg, y = Q_model),
            colour = "#E07B54", linewidth = 1) +
  theme_bw() +
  xlab("Stage (cm)") + ylab("Discharge (m³/s)") +
  labs(title = "RC3 -- full power law fit",
       subtitle = paste0("a = ", round(RC3_pl_params["a"], 4),
                         ", b = ", round(RC3_pl_params["b"], 4),
                         ", h0 = ", round(RC3_pl_params["h0"], 1), " cm"))
