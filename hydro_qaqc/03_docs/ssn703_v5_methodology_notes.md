# SSN703 Discharge Dataset — Version 5 Methodology Notes

## Overview

The discharge record for watershed SSN703 has been updated to incorporate data from 2019-02-09 onward using a revised rating curve pipeline (v5). The previous pipeline (v4, last updated 2019-02-09) applied three successive rating curves (Ratings 1–3) to stage data collected at loc_1 using pressure transducers ssn703_a and ssn703_b. The v4 stage record was reviewed and found to be correctly processed; all discharge values prior to 2019-02-09 remain as originally computed and no back-calculation was performed.

## Sensor and Location History

| Period | Sensor | Location | Notes |
|:-------|:-------|:---------|:------|
| 2012 to 2017-10-12 | ssn703_a | loc_1 | Original sensor; began recording noisy data |
| 2017-10-12 to 2018-09-14 | ssn703_b | loc_1 | Installed beside ssn703_a; -1cm then -2cm offset applied by v4 pipeline |
| 2018-09-14 to 2023-06-25 | ssn703_c | loc_2 | New physical location; never incorporated into v4 pipeline |
| 2021-09-02 to present | ssn703_d | loc_3 | Installed alongside ssn703_c; autosalt db pulled stage from ssn703_c until 2023-09-15 |
| 2023-09-15 onward | ssn703_d | loc_3 | Authoritative sensor from this date |

Note: ssn703_a and ssn703_b are co-located at loc_1. The offset corrections applied in v4 were estimations to account for sensor swaps; the stage record at loc_1 was reviewed and accepted as correctly processed.

## New Rating Curves

This update introduces two new rating curves covering the period from 2019-02-09 onward:

| Rating | Sensor | Location | Applied from | Gaugings | Method | Status |
|:-------|:-------|:---------|:-------------|:---------|:-------|:-------|
| Rating 4 (RC2) | ssn703_c | loc_2 | 2019-02-09 | 103 | LOESS + constrained power law extrapolation | Final |
| Rating 5 (RC3) | ssn703_d | loc_3 | 2023-09-15 | 18 | Full power law | Provisional |

Both curves were developed using the same bootstrap confidence interval methodology established in v4 (Coxon et al. 2015). RC2 used LOESS with span = 0.14 and power law extrapolation above 150cm inflection point, capped at 218cm (maximum recorded stage). RC3 used a full power law fit due to insufficient data for LOESS, extended from h₀ to 218cm.

## Low Flow Extrapolation (RC3)

The minimum gauged stage at loc_3 is 56.88cm. The RC3 power law curve has been extended below this to h₀ (42.6cm) to provide discharge estimates during low flow periods. This extrapolation is physically grounded — the power law naturally approaches zero at h₀ — but carries higher uncertainty than the fitted range (±20% CI below 56.88cm vs ±10% within the gauged range). No gaugings exist at loc_3 below 56.88cm. Collecting low flow gaugings with concurrent staff gauge readings at the loc_3 PT is recommended as a field priority for the next dry season visit.

## Known Gaps and Uncertainties

**Boundary step change (2019-02-09):**
A small step change in discharge exists at the 2019-02-09 boundary, particularly at low flows. This reflects the genuine hydraulic difference between loc_1 and loc_2 and is not a processing artefact. High flow peaks are consistent across the boundary. Users performing low flow analysis or computing total volumes that span this date should be aware of this discontinuity.

**ssn703_d overlap period (2021-09-02 to 2023-09-15):**
ssn703_d was physically installed at loc_3 on 2021-09-02 and began recording stage from that date. However the autosalt database continued pulling stage from ssn703_c (loc_2) until 2023-09-15. This means ssn703_d stage data exists from 2021-09-02 onward in the stage QC files, but it is not the authoritative stage source for discharge computation until 2023-09-15. The RC3 discharge file is filtered to start at 2023-09-15 in `10_discharge.R` to ensure one clean discharge value per timestamp with no overlap between RC2 and RC3. The ssn703_d stage data from 2021-09-02 to 2023-09-15 exists in the per-sensor stage QC files for reference but is not used in the production discharge record.
During this period, ssn703_c had been installed at loc_2 and the logger was pointed to ssn703_c at the time of install (2018-09-14). However the v4 pipeline continued using ssn703_b at loc_1 and never incorporated ssn703_c. Discharge values for this window in the existing database were therefore computed using ssn703_b stage at loc_1 despite ssn703_c being the active sensor from 2018-09-14 onward. This period predates the v5 pipeline and is not corrected here.

**November 2023 data gap (~35 days):**
Stage data from ssn703_c was unreliable during the transition to ssn703_d. Approximately 10,080 timesteps (35 days) are flagged as `unfilled` and carry NA discharge values in the 2023-2024 water year record.

**RC3 provisional status:**
RC3 is based on 18 gaugings over approximately two water years with limited coverage in the 80-140cm stage range which drives the majority of annual runoff volume. Annual runoff coefficients (0.58-0.68) are slightly below the historical average of ~0.70 from the v4 record; this is partly attributable to sparse gauging coverage and will improve as additional measurements are collected. RC3 should be updated when a minimum of 10 additional gaugings have been collected, prioritising the 80-140cm stage range and low flow conditions.

**Water year 2020-2021 excluded from RC2 fitting:**
Sparse and biased stage coverage during this water year made it unsuitable for curve fitting. The RC2 curve extrapolates through this period without direct gauging validation.

## Discharge Record Quality Flags

The `discharge_flag` column in the output files mirrors the stage QC flag:

| Flag | Meaning |
|:-----|:--------|
| `raw` | Measured stage, no gap-filling applied |
| `gf_spline` | Gap-filled using spline interpolation |
| `gf_spline_event` | Gap-filled across a storm event |
| `unfilled` | Stage gap could not be filled — discharge is NA |
| `out_of_range` | Stage outside rated range — discharge is NA |

## Rainfall-Runoff Validation

Annual runoff coefficients were computed using the 600m ASL rain gauge (ssn693703) as the precipitation input, consistent with v4 methodology. Orographic effects at this coastal site make the 600m gauge more representative of catchment-average rainfall than the 50m gauge.

**RC2 validation (2019-2023):**

| Water year | Runoff coefficient | Notes |
|:-----------|:-------------------|:------|
| 2019-2020 | 0.725 | |
| 2021-2022 | 0.641 | |
| 2022-2023 | 0.630 | |

WY 2020-2021 excluded due to rain gauge gaps. Coefficients consistent with historical average of ~0.70.

**RC3 validation (2023 onward):**

| Water year | Runoff coefficient | Notes |
|:-----------|:-------------------|:------|
| 2023-2024 | 0.681 | 9.6% NA discharge (Nov 2023 gap) |
| 2024-2025 | 0.583 | High rainfall year (5130mm) |
| 2025-2026 | 0.549 | Incomplete water year |

RC3 coefficients slightly below historical average — consistent with provisional curve status and sparse gauging coverage in the 80-140cm range.

## File Outputs

| File | Description |
|:-----|:------------|
| `ssn703_discharge_RC2.csv` | RC2 discharge, 2019-02-09 to 2023-06-25, 5-minute resolution |
| `ssn703_discharge_RC3.csv` | RC3 discharge, 2023-09-15 onward, 5-minute resolution |
| `ssn703_discharge_combined.csv` | RC2 + RC3 combined, 2019-02-09 onward |
| `ssn703_rating_curve_lookup_combined.csv` | All five ratings (v4 Ratings 1-3 + new Ratings 4-5) |
| `ssn703_RC2_rating_curve_v1.csv` | RC2 lookup table with CI |
| `ssn703_RC3_rating_curve_v1.csv` | RC3 lookup table with CI and extrapolated_low flag |

## Recommended Next Steps

1. Collect low flow gaugings at loc_3 with concurrent PT staff gauge readings (dry season priority)
2. Target 80-140cm stage range at loc_3 for additional gaugings to improve RC3 mid-range coverage
3. Update RC3 when minimum 10 additional gaugings are available
4. **Develop rating curve for SSN844 and compare 2024-2025 annual runoff coefficient against SSN703 RC3 value of 0.583** -- if SSN844 also shows a lower-than-historical coefficient for that year, the lower RC3 value is likely a real hydrological signal rather than a curve uncertainty issue. If SSN844 is close to historical average (~0.7), the RC3 curve may be slightly underestimating discharge in the 80-140cm range.
5. Investigate 2024-2025 low runoff coefficient further as additional data becomes available
6. Formal uncertainty budget propagation through stage QC and gap-filling steps (future work)

---
*Generated: `r Sys.Date()` | Pipeline: v5 | Author: [your name]*
