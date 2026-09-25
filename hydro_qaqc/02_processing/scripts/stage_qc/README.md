# stage_qc/ — SSN703 stage quality control

One place for stage QC. Built from the old `qc-functions.R` + `pls-workflow.R`
(the cleanest of the three parallel stage-QC implementations that had grown up in
`02_processing/scripts/`), generalised across all four SSN703 pressure-transducer
generations, with the datum offset corrected.

## Files

| File | What it is |
|---|---|
| `stage_qc_functions.R` | Function library. `sn_*` = Hakai API read, `stage_*` = local raw-CSV read, `qc_*` = diagnostics / triage plots / post-QC review. Source this first. |
| `stage_qc_pipeline.R` | The QC engine — `stage_qc_run(raw, sa, params)`: one sensor generation in, QC'd series + flags out. Datum offset → range → flatline → bad-data → SA gap-fill → SA reconstruction → spline gap-fill → `qc_flag`. |
| `run_stage_qc_ssn703.R` | SSN703 driver. Builds per-generation config from `sensor_registry.csv` + `offsets.csv`, runs the engine, writes the outputs. |

## Run

From the **project root**:

```r
source("02_processing/scripts/stage_qc/run_stage_qc_ssn703.R")
```

`run_stage_qc_ssn703.R` sources the other two itself. Defaults read the local
per-sensor CSVs in `01_raw/SSN703/` (unambiguous, offline). Set `PRIMARY_SOURCE`
/ `SA_SOURCE` to `"api"` at the top of the run script for a live Hakai pull.

## Outputs (the downstream contract)

- `04_outputs/per_sensor/ssn703_<sensor>_stage_qc.csv` — columns
  `station_id, location_id, rating_curve_period, timestamp, water_year,
  stage_corrected, offset_applied, qc_flag, qc_flag_detail, site_id`.
  `qc_flag` uses the downstream vocabulary (`raw`/`gf_sa`/`gf_spline`/`bad_data`/
  `unfilled`); `qc_flag_detail` keeps the engine's full label. **`10_discharge.R`,
  `11_discharge_summary.R`, `upload_ready.R`, and
  `rating_curve/fit_rating_curves.Rmd` all read this file — keep the column set
  and the `timestamp` (UTC ISO8601) / `water_year` (`YYYY-YYYY`) formats stable.**
- `04_outputs/per_rc/ssn703_RC{1,2,3}_stage_qc.csv` — chronological, de-overlapped.
- `pls{,2,3,4}_for_db_SSN703US_<range>.rds` (repo root) — Hakai QC upload format
  (`measurement_time, quality_level, qc_flag, measurement_name, qc_by,
  recorded_time, avg`). `measurement_name` is per generation —
  `PLS_Lvl` / `PLS2_Lvl` / `PLS3_Lvl` / `PLS4_Lvl` (ssn703_a/b/c/d) — all posted
  to the one QC table `ssn703us_5minute`. Review before POSTing; the POST loop is
  in the archived `pls-workflow.R` §11.
- `02_processing/plots/ssn703_<sensor>_qc_review.pdf`, `ssn703_offset_validation.pdf`,
  and `ssn703_<sensor>_sa_recon.pdf` where a bad-data window was rebuilt.

## Where the knobs are

- **QC thresholds** — `stage_qc_default_params()` in `stage_qc_pipeline.R`, with
  per-generation overrides in `build_config()` in the run script.
- **Datum offset** — `03_docs/metadata/offsets.csv`. The run script applies the
  recorded value across the failing sensor's whole deployment window. Currently:
  `ssn703_b` gets `-0.019 m` onto the `ssn703_a` (loc_1 / MK) datum; `ssn703_a`,
  `ssn703_c`, `ssn703_d` get none. See `memory/mk-rc1-stage-provenance.md` and
  `rating_curve/recreate_mk_curve.R` Part B for why.
- **Deployment / bad-data windows** — `03_docs/metadata/sensor_registry.csv`.

## Notes

- **Spike detection is intentionally off** (`stage_qc_pipeline.R` §4). Every
  earlier version flagged real hydrological rises as spikes. Re-enable only with a
  persistence / recovery check.
- Pre-threshold diagnostics (`qc_summarise_gaps`, `qc_rate_of_change_quantiles`,
  `qc_plot_roc_histogram`, `qc_plot_overlap`) are meant to be run interactively
  from `stage_qc_functions.R` before you touch thresholds.
