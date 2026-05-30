# This function adds the uncertainty in stage to discharge uncertainty
# x = HQ
# y = Rating (LOESS fit)
# z = Rating_extrap_interp (interpolated full curve including extrapolation)
#
# Updated to use approx() instead of merge() to handle duplicate stage values
# which occur when multiple gaugings fall at the same stage to 1 decimal place.
# The original merge() approach dropped duplicate rows causing row count mismatches
# downstream in CI_model_input() and CI().

HQ_unc <- function(x, y, z) {
  HQ_unc <- x |>
    dplyr::select(Stage_avg, Stage_stdv, Q_meas, Q_rel_unc) |>
    mutate(
      Q_model          = approx(y$Stage_avg, y$Q_model,
                                xout = Stage_avg, rule = 1)$y,
      Stage_add_stdv   = Stage_avg + Stage_stdv,
      Q_model_add_stdv = approx(z$Stage_avg, z$Q_model,
                                xout = Stage_avg + Stage_stdv, rule = 1)$y,
      Rel_unc_stage    = (Q_model_add_stdv - Q_model) / Q_model * 100,
      Rel_unc_stage    = ifelse(Rel_unc_stage < 0, 0, Rel_unc_stage),
      Q_H_rel_unc      = sqrt((Q_rel_unc^2) + (Rel_unc_stage^2)),
      Q_H_abs_unc      = (Q_H_rel_unc / 100) * Q_meas,
      Q_max            = Q_meas + Q_H_abs_unc,
      Q_min            = Q_meas - Q_H_abs_unc
    ) |>
    arrange(Stage_avg) |>
    dplyr::select(Q_meas, Q_max, Q_min)
  
  return(HQ_unc)
}