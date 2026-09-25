# sn_generic_qc/ — generic sensor-network QC + upload

The download → QC → upload workflow for **any** Hakai Sensor Network 5-minute
measurement, not just SSN703 stage. Replaces the ad hoc, one-off pattern
(hand-written column-rename block per site, colon-prefixed API column names
touched directly, `pls-workflow.R`-style copy/paste per variable) with one
reusable path. Does not touch `stage_qc/`, `rating_curve/`, or the archived
`pls-workflow.R` — separate, parallel engine.

## Files

| File | What it is |
|---|---|
| `sn_discovery.R` | `sn_find_views` / `sn_find_components` — look up a site/view/component without memorizing exact strings. `sn_read_measurement()` — download a component's full `value`/`value_min`/`value_max`/`value_std` family in one call. Sources `stage_qc_functions.R` first (reuses `sn_read_values` / `sn_read_qc` / `sn_qc_table_name` unmodified). |
| `qc_generic_pipeline.R` | `qc_run()` — the QC engine. Same 7-step structure as `stage_qc_pipeline.R` (offset → range → flatline → bad-data → reference gap-fill → reference reconstruction → spline gap-fill → `qc_flag`), generalised: no default range/flatline thresholds, optional `reference` series instead of an assumed water-level SA. |
| `qc_generic_upload.R` | `qc_format_upload()` / `qc_validate_upload()` / `sn_post_qc()` — the confirmed `sn/qc/:tableName` upload shape, a hard validation gate, and the PATCH/POST loop. |
| `run_generic_qc_pilot.R` | Pilot across 3 variables (RH, air temp, a second-station stage) — discover→download→QC→format→validate end to end. Never uploads. |
| `test_generic_qc.R` | Manual, edit-SITE/VIEW/COMPONENT-and-rerun scratch script, one variable at a time. Never uploads unless you uncomment the last section. |

## Run

From the **project root**:

```r
source("02_processing/scripts/stage_qc/stage_qc_functions.R")   # sn_read_values / sn_read_qc / sn_qc_table_name
source("02_processing/scripts/sn_generic_qc/sn_discovery.R")
source("02_processing/scripts/sn_generic_qc/qc_generic_pipeline.R")
source("02_processing/scripts/sn_generic_qc/qc_generic_upload.R")
```

Then either `run_generic_qc_pilot.R` (multi-variable smoke test) or
`test_generic_qc.R` (one variable, edit and re-run).

## Notes

- **Download shape** — the raw `sn/views` endpoint returns colon-prefixed,
  site-qualified column names (`"SSN819US:PLS2_Lvl_QC"`), one column per
  site/component. `sn_read_values()` (`stage_qc_functions.R`, reused here)
  reshapes that into a tidy long frame once, at read, so nothing downstream
  touches an API column-name string.
- **Burst stats (Avg/Min/Max/Std) are carried natively** — `qc_run(primary =
  ...)` takes `sn_read_measurement()`'s output directly
  (`measurement_time`/`value`/`value_min`/`value_max`/`value_std`); the three
  stats get the same offset/MV treatment `value` does and come back as
  columns on the result. `qc_format_upload(..., include_stats = TRUE)` reads
  them straight off that — no separate join step. Passing a bare
  `measurement_time`/`value` series still works; the stats just come back
  `NA`.
- **Upload schema** — the default 7 columns (`measurement_time`,
  `quality_level`, `qc_flag`, `val`, `measurement_name`, `qc_by`,
  `recorded_time`) are confirmed against a real, executed `client$patch()`
  call. `include_stats = TRUE` adds `unesco_q_level`/`min`/`max`/`std` on top
  — **not confirmed against the live API**, opt-in only. Confirm separately
  (small scratch window, check the response) before trusting it for a real
  upload.
- **PATCH vs. POST** — `sn_post_qc(..., method = c("patch", "post"))`, your
  choice. `"patch"` is the default, matching the one confirmed real-world
  script; Hakai's API may accept either depending on intent (POST to add new
  records, PATCH to modify existing ones). Always runs
  `qc_validate_upload()` first and refuses to upload on any problem row.
- **Not covered here** — `upload_ready.R`'s Telemetry-Network CSV format
  (`WtrLvl<gen><site>`-prefixed columns, 4-row header) is a different upload
  path with a per-site column-naming scheme that isn't a fixed formula.
  Deliberately not generalised in this folder.
