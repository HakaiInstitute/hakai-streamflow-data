# rating_curve/ — SSN703 rating curve development

Consolidates what used to be spread across `06_gauging_table_prep.R` + `07a` + `07b`,
`07_rating_curve_exploration.R`, `RC3_power.R`, `rc1vrc2.R` /
`rc1vrc2_shared_exponent.R` / `rc1vrc2_robustness.R`, `recreate_MK_curve.R`, six
root `08_rating_curve_*.Rmd` files, and the colleague's helper scripts
(`selectspan.R`, `interp1.R`, `interp2.R`, `HQ_unc.R`, `CI.R`, `CI_model_input.R`).

## Files

| File | Was | What it does |
|---|---|---|
| `rating_curve_functions.R` | selectspan / interp1 / interp2 / HQ_unc / CI / CI_model_input + code inlined in the Rmd | LOESS spans, power-law fit, anchored extrapolation, mm interpolation, stage-uncertainty propagation (**keyed** — no row-order bugs), bootstrap mixture-normal CI (`rc_qmixnorm` inline — no KScorrect), extrapolated-CI anchor/widen, pooled low-flow uncertainty trend. Source this first. |
| `prep_gaugings.R` | 06 + 07a + 07b | Builds `ssn703_gaugings_prepped.csv`: RC-period assignment, **no datum offset** (see below), RC1-tail recovery from MK's original table, and the optional RC2 refit-calibration extension. |
| `explore_gaugings.R` | 07_rating_curve_exploration.R | Exploratory stage-vs-Q plots (HTML + PDF). |
| `fit_rating_curves.Rmd` | 08_rating_curve_v5.2.Rmd (current) | RC2 + RC3 fitting. Writes the `04_outputs/` lookup CSVs. |
| `compare_rc1_rc2.R` | rc1vrc2*.R (×3) | Shared-exponent / LOO / window-sensitivity / residual tests for the RC1↔RC2 step. `TESTS` and `WINDOW` at the top. |
| `recreate_mk_curve.R` | recreate_MK_curve.R | Reproduces MK's RC1 (Part A) + the stage-provenance check (Part B). **The regression guard for the datum-offset direction.** |

## Run order

```r
source("02_processing/scripts/rating_curve/prep_gaugings.R")      # -> ssn703_gaugings_prepped.csv
source("02_processing/scripts/rating_curve/explore_gaugings.R")   # review plots
rmarkdown::render("02_processing/scripts/rating_curve/fit_rating_curves.Rmd")
source("02_processing/scripts/rating_curve/compare_rc1_rc2.R")    # optional diagnostic
source("02_processing/scripts/rating_curve/recreate_mk_curve.R")  # standalone check
```

All read/write paths are relative to the **project root** — run from there.

## ACTION REQUIRED steps in `fit_rating_curves.Rmd`

- `RC2_SPAN` — chosen LOESS span (chunk `rc2_span_choose`).
- `RC2_INFLECTION` — stage above which gaugings fit the high-end power law
  (chunk `rc2_extrap`).
- Whether to swap `RC3_pl_CI` → `RC3_pl_CI_v2` (fitted low-flow CI) in `rc3_save`.

## Datum

`prep_gaugings.R` adds **no offset** to the gauging table. MK's `Stage_avg`
through 2018-09-14 is already on her ssn703_a reference datum; the RC1 tail is
recovered from her own original table (also already adjusted). The ~−1.9 cm
ssn703_b correction is a *continuous-series* fix and lives in
`stage_qc/` + `03_docs/metadata/offsets.csv`, not here. RC2 and RC3 are separate
locations and are not affected by any RC1 datum choice.

See `memory/mk-rc1-stage-provenance.md`.

## Inputs / outputs

- **Inputs:** `03_docs/metadata/ssn703_gaugings_raw.csv`,
  `MK_original_gauging_table_703.csv`, `ssn703_RC1_lookup_v_previous.csv`,
  `offsets.csv`; `04_outputs/per_sensor/ssn703_ssn703_c_stage_qc.csv` (RC2 low-end
  target — produced by `stage_qc/run_stage_qc_ssn703.R`).
- **Outputs:** `03_docs/metadata/ssn703_gaugings_prepped.csv` (+ dated backup),
  `ssn703_rc2_refit_calibration.csv`; `04_outputs/ssn703_RC2_rating_curve_v1.csv`,
  `ssn703_RC3_rating_curve_v1.csv`, `ssn703_RC{2,3}_stagedischarge_v1.csv`,
  `ssn703_rating_curve_lookup_combined.csv` (→ consumed by `10_discharge.R`),
  `04_outputs/curve_validation/*`.
