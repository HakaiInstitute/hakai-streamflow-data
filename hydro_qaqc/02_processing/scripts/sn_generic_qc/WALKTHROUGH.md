# Walkthrough — understanding the sn/stage QC workflow

A reading order for `stage_qc/` + `sn_generic_qc/` together, built around how the
pieces actually depend on each other rather than folder order —
`sn_generic_qc/` can't be understood without `stage_qc_functions.R` first,
since it reuses half of it unmodified. See also `stage_qc/README.md` and
`sn_generic_qc/README.md` for the reference-doc version of this (file roles,
run instructions, output contracts); this is the *read-it-in-this-order* version.

## 0. Orient (5 min)

Read `stage_qc/README.md` and `sn_generic_qc/README.md` first.

Mental model to hold onto: **two parallel QC engines sharing one
foundation.** `stage_qc/` is the original, SSN703-only, hardcoded-thresholds
pipeline. `sn_generic_qc/` is the generalized version (any site, any
variable, no assumed thresholds) that was built *from* it — same shape,
deliberately kept step-for-step identical where possible. That pays off in
Phase 3 below.

## 1. The shared foundation — `stage_qc/stage_qc_functions.R`

Everything else sources this first. It's organized in 5 lettered sections
(the file's own header, lines 19-24) — walk it in that order:

- **A. `sn_*` (Hakai API access)** — `sn_connect`, `sn_read_values`,
  `sn_qc_table_name`, `sn_read_qc`, `sn_join_qc`, `sn_read_station`. This is
  the part `sn_generic_qc/` reuses unmodified. Pay particular attention to
  **`sn_qc_table_name()`** (~line 113) — it's the site-to-table-name
  mapping, the thing that silently determines *where* an upload lands (a
  real bug here — the outlet/trib underscore rule — was found and fixed
  2026-09-24: without it, every `W50_*Trib*`/`W50_*Outlet*` site resolved to
  a QC table name that didn't exist).
- **B. `stage_*`** — local raw-CSV loading, the offline fallback path.
  Skippable on a first pass if you only care about the live-API workflow.
- **C. `qc_*` pre-threshold diagnostics** — gap-length summaries,
  rate-of-change quantiles. Meant to be run interactively *before* you set
  thresholds, not part of the automated pipeline.
- **D. `qc_*` pre-flagging triage — `qc_plot_diagnostic()` /
  `qc_plot_overlap()`.** `qc_plot_diagnostic()` (~line 451) takes a
  `value_label` param (added 2026-09-25) so the y-axis shows the actual
  variable being QC'd instead of always "Stage" — worth running against
  real data to see it in effect.
- **E. `qc_*` post-flagging review** — `qc_summarise_chunks()` and friends,
  for reviewing what a QC run actually did after the fact.

**Checkpoint:** run `sn_connect()`, then `sn_read_values()` against one real
site:view:component, then `qc_plot_diagnostic()` on the result. Exercises
sections A and D together and gets live data on screen.

## 2. Discovery layer — `sn_generic_qc/sn_discovery.R`

Only exists because the generic path can't hardcode site/component strings
the way `run_stage_qc_ssn703.R` does. Three functions, each building on the
last:

- `sn_list_views()` / `sn_find_views()` — site/view lookup by pattern
  instead of memorized string. **Fixed 2026-09-24**: was hitting a
  404'ing endpoint (`api/sn/views` instead of the real `api/sn/views/list`)
  — worth re-reading now that you know why.
- `sn_resolve_site()` — the fuzzy-match fallback `sn_list_components()`
  calls when a direct probe fails. Auto-resolves an unambiguous near-miss
  (e.g. `"SSN844"` when only `"SSN844US"` exists); lists candidates and
  stops on a real ambiguity (e.g. `"SSN844"` when `SSN844DS`, `SSN844PWR`,
  and `SSN844US` all exist).
- `sn_list_components()` / `sn_find_components()` / `sn_read_measurement()`
  — what's actually inside a site:view, and pulling the full
  Avg/Min/Max/Std family in one call.

**Checkpoint:** `sn_find_views(client, "<something you know exists>")` then
`sn_find_components()` on the result — the whole discovery chain in two
calls.

## 3. The two QC engines, side by side

Worth reading in a split view — they're intentionally parallel:

| Step | `stage_qc/stage_qc_pipeline.R` → `stage_qc_run()` | `sn_generic_qc/qc_generic_pipeline.R` → `qc_run()` |
|---|---|---|
| 1 | Datum offset (~line 148) — **stage/SSN703-only, no generic equivalent** | — |
| 2 | Range check (~165) | Range check (~195) |
| 3 | Flatline check (~180) | Flatline check (~218) |
| 4 | Spike detection — **disabled**, same reason both places | Spike detection — disabled (~267) |
| 5 | Gap-fill Tier 1: SA linear relationship (~233) | Gap-fill Tier 1: generic `reference` series (~274) |
| 6 | Gap-fill Tier 2: spline (~365) | Gap-fill Tier 2: spline (~408) |
| 7 | Combine into `qc_flag` (~453) | Combine into `qc_flag` (~499) |

Read `stage_qc_run()` first (the original, most comments explaining *why*),
then `qc_run()` immediately after and just note the diffs — mostly
"hardcoded SA/stage assumption" → "generic `reference` param, no default
thresholds." Faster than reading `qc_run()` cold.

**Checkpoint:** diff `qc_default_params()` against `stage_qc_default_params()`
— tells you exactly what's stage-specific vs universal.

## 4. Upload — `sn_generic_qc/qc_generic_upload.R`

Three lettered sections, meant to be read as a pipeline:

- **A. `qc_format_upload()`** (~line 86) — builds the 7-column upload
  tibble (`measurement_time`, `quality_level`, `qc_flag`, `val`,
  `measurement_name`, `qc_by`, `recorded_time`).
- **B. `qc_validate_upload()`** (~line 179) — the guardrail. This is where
  "how does the DB know what site it's uploading to" gets answered in
  code: site identity comes from the URL (`table_name`, built by
  `sn_qc_table_name()`), not from any column in the row data —
  `measurement_name` is the second half of the addressing (one table can
  hold several measurement types for a site), and this function optionally
  cross-checks it against that specific `table_name`'s real history.
- **C. `sn_post_qc()`** (~line 355) — batches (default 1000 rows/PATCH) +
  sends, but *only* after B passes with 0 problems. Calls
  `sn_qc_table_name()` from Phase 1 to build the URL — this is where that
  function's correctness actually matters.

The `stage_qc/` side has no equivalent formalized module — its upload step
lives partly in `run_stage_qc_ssn703.R`'s output and partly in the archived
`pls-workflow.R` §11 POST loop. That asymmetry is deliberate, not a gap to
go looking to fill.

## 5. Drivers — how it's actually invoked

- **`stage_qc/run_stage_qc_ssn703.R`** — builds per-sensor-generation config
  from `sensor_registry.csv`/`offsets.csv`, then calls `stage_qc_run()` once
  per generation. Read this to see `stage_qc_pipeline.R` used for real.
- **`sn_generic_qc/run_generic_qc_pilot.R`** — multi-variable smoke test
  (RH, air temp, a second stage site), discover→download→QC→format→validate,
  never uploads. Good end-to-end read since it touches every file above in
  sequence.
- **`sn_generic_qc/test_generic_qc.R`** — the one you edit-and-rerun by hand
  for one variable at a time. Your scratchpad once you understand the rest —
  not really a "read" file so much as a "keep open and edit" file.

## 6. Where it feeds (context, not part of this workflow itself)

`stage_qc/`'s output contract (per its README) feeds `10_discharge.R`,
`11_discharge_summary.R`, `upload_ready.R`, and
`rating_curve/fit_rating_curves.Rmd`. Good to know the boundary exists so
you don't go looking for rating-curve logic inside the QC folders — it's a
deliberately separate, downstream workflow.

## Appendix — recently touched, worth double-checking against live data

These changed this week; re-running each against real data once is a good
way to confirm you understand what changed and why:

- `sn_qc_table_name()` (`stage_qc/stage_qc_functions.R`) — outlet/trib
  underscore fix.
- `sn_list_views()` / `sn_resolve_site()` (`sn_generic_qc/sn_discovery.R`) —
  real endpoint fix + fuzzy site fallback.
- `qc_plot_diagnostic()` (`stage_qc/stage_qc_functions.R`) — new
  `value_label` param, wired through `run_generic_qc_pilot.R` and
  `test_generic_qc.R`.
