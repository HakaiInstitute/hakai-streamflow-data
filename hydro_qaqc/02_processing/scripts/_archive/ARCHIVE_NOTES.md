# _archive/ — superseded SSN703 stage-QC and rating-curve scripts

Moved here (not deleted) when the SSN703 stage-QC and rating-curve scripts were
consolidated into `../stage_qc/` and `../rating_curve/`. Nothing here is on a
current run path. The SSN844 pipeline is unchanged and still lives in the parent
directory.

## Stage QC — replaced by `../stage_qc/`

| Archived | Replaced by |
|---|---|
| `01_load_stage_data.R` | `stage_qc/stage_qc_functions.R` (`stage_read_raw_csv()`, `stage_read_registry()`) |
| `02_inspect_stage.R` | `stage_qc/stage_qc_functions.R` (`qc_plot_overlap()`) + the pre-QC diagnostics block |
| `03_offset_calculation.R` | `stage_qc/run_stage_qc_ssn703.R` (offset now read from `offsets.csv`, applied in the pipeline) |
| `04_stage_qc.R`, `04_stage_qc_v2.R` | `stage_qc/stage_qc_pipeline.R` |
| `05_stage_output.R` | `stage_qc/run_stage_qc_ssn703.R` (per-sensor / per-RC writers) |
| `703_stage_qc.R`, `703_stage_qc_load.R`, `703_stage_qc_automated.R`, `703_stage_qc_automated_plot_review.R` | folded into `stage_qc/run_stage_qc_ssn703.R` (PLS4 / ssn703_d is one generation in the loop) |
| `qc-functions.R` | `stage_qc/stage_qc_functions.R` (verbatim, reorganised) |
| `sn-functions.R` | duplicate of `qc-functions.R` section A — same as above |
| `qc_diagnostics.R`, `qc_plot_diagnostic.R`, `qc_review.R`, `plot_sensor_qc_flags.R` | partial copies of `qc-functions.R` sections B/C/D — all in `stage_qc/stage_qc_functions.R` |
| `pls-workflow.R` | `stage_qc/stage_qc_pipeline.R` + `run_stage_qc_ssn703.R`. **The DB upload POST loop is in section 11 of this file** — copy it out when you're ready to upload. |
| `pls_qc_for_upload_example.R` | superseded by `run_stage_qc_ssn703.R` (which produces `pls{,2,3,4}_for_db_*.rds`) |

## Rating curve — replaced by `../rating_curve/`

| Archived | Replaced by |
|---|---|
| `06_gauging_table_prep.R` + `07a_recover_historical_stage_rc1.R` + `07b_Exentd_RC2_Cal.R` | `rating_curve/prep_gaugings.R` (one script, explicit stages). **The +2 cm ssn703_a-era offset is dropped — see the datum note in that script.** |
| `07_rating_curve_exploration.R` | `rating_curve/explore_gaugings.R` |
| `RC3_power.R` | `rating_curve/rating_curve_functions.R` (`rc_fit_powerlaw()`), used in `fit_rating_curves.Rmd` |
| `rc1vrc2.R`, `rc1vrc2_shared_exponent.R`, `rc1vrc2_robustness.R` | `rating_curve/compare_rc1_rc2.R` (one script, `TESTS` / `WINDOW` switch) |
| `recreate_MK_curve.R` | `rating_curve/recreate_mk_curve.R` (moved, now sources `rating_curve_functions.R` for `rc_qmixnorm`) |
| `# SSN703 — Sensor History & Rating Curve.R` | scratch exploration, superseded by the above |

## Still in the parent directory on purpose

- `selectspan.R`, `interp1.R`, `interp2.R`, `HQ_unc.R`, `CI.R`, `CI_model_input.R`
  — the colleague's helpers. The SSN703 curve fitting no longer uses them
  (`rating_curve/rating_curve_functions.R` is the clean replacement), but
  `08_rating_curve_fit_844.Rmd` and `08b/08c_*_844.R` still `source()` them.
  Remove once SSN844 is migrated onto the new structure.
- All `*_844*` scripts, `00_build_metadata.R`, `09_rainfall_runoff_check.R`,
  `10_discharge.R`, `11_discharge_summary.R`, `13*`, `hakai-*.R`, `upload_ready.R`
  — unchanged, still on the run path.
