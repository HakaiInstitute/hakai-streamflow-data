# SSN844 Discharge Dataset — Version 2 Methodology Notes

## Overview

The discharge record for watershed SSN844 has been updated to incorporate data from 2017-07-13 onward using a revised rating curve pipeline (v2). The previous pipeline (v1, last updated 2017-07-13) applied two successive rating curves (Ratings 1–2) to stage data collected at loc_1 using pressure transducer ssn844. The v1 stage record was reviewed and found to be correctly processed; all discharge values prior to 2017-07-13 remain as originally computed and no back-calculation was performed.

The key update in v2 is the extension of Rating 2 from its previous maximum of 146cm to 160cm (the maximum recorded stage), and the incorporation of new autosalt gaugings collected between 2018 and 2024 to validate the existing curve.

## Sensor and Location History

| Period | Sensor | Location | Notes |
|:-------|:-------|:---------|:------|
| 2014-09-10 to present | ssn844 | loc_1 | Single sensor; no location changes throughout record |
| 2018-09-02 to 2024-05-09 | ssn844_sa | loc_2 | Supplementary sensor; used for gap filling only; not used in discharge computation |

Note: SSN844 has a single primary sensor location throughout its entire record. No datum corrections or offset adjustments are required.

## Rating Curves

| Rating | Sensor | Location | Applied from | Applied to | Method | Status |
|:-------|:-------|:---------|:-------------|:-----------|:-------|:-------|
| Rating 1 | ssn844 | loc_1 | 2012-01-01 | 2017-07-12 | Previous pipeline (v1) | Retained as-is |
| Rating 2 | ssn844 | loc_1 | 2017-07-13 | present | Previous curve extended to 160cm | Updated |

Rating 2 was originally developed in the v1 pipeline. In v2, the curve has been extended from its previous maximum of 146cm to 160cm using a power law fit (Q = a(h - h₀)^b) back-calculated from the existing lookup table. The fitted coefficients are:

- a = 0.000274
- b = 2.2787
- h₀ = 31.4 cm

The extension above 146cm uses a constant CI offset based on the CI width at 146cm, the last gauged point in the original curve.

## Gauging Validation

New autosalt gaugings collected between 2018 and 2024 (168 events, after exclusions) were compared against the existing Rating 2 curve. The diagnostic assessment showed:

- Mean residual: 0.117 m³/s
- Mean gauging uncertainty: 0.202 m³/s
- The mean residual is smaller than the mean measurement uncertainty, indicating no systematic bias in the existing curve
- Scatter increases above ~110cm but shows no consistent directional bias
- Decision: Rating 2 is retained with extension only; no new curve fitted

One gauging was excluded from the validation dataset:

| EventID | Date | Stage | Q | Reason |
|:--------|:-----|:------|:--|:-------|
| 975031801 | 2020-11-24 02:37 | 91.5cm | 2.23 m³/s | No coherent salt wave on EC1; baseline noise throughout |

## Stage QC

The SSN844 stage record was quality-controlled using a tiered approach:

**Known bad data periods (hard-coded):**

| Period | Duration | Nature | Fill method |
|:-------|:---------|:-------|:------------|
| 2021-06-27 12:45 to 2021-07-19 11:00 | ~22 days | Slow drift/blockage; gradual climb from ~0.35m to ~1.0m; missed by rate-of-change algorithm | SA sensor (replaced_sa) |
| 2023-06-05 to 2023-06-11 | ~7 days | Sharp spike | SA sensor (replaced_sa) |
| 2023-07-27 10:50 to 2023-07-30 17:35 | ~3 days | Sharp spike | SA sensor (replaced_sa) |

**Algorithmic spike detection:**
Rate-of-change threshold of 0.03m per 5 minutes and rolling median deviation check applied to all remaining raw data.

**Gap filling tiers:**
1. SA sensor relationship (R² threshold 0.95) — applied where ssn844_sa data available
2. Spline interpolation for gaps ≤ 3 hours
3. Gaps > 3 hours without SA coverage left as NA (`unfilled`)

## Known Gaps and Uncertainties

**Upper rating curve extrapolation (above 146cm):**
The original Rating 2 lookup table extended to 146cm. The v2 extension to 160cm is based on power law extrapolation beyond the gauged range. No gaugings exist above ~143cm. The maximum recorded stage of 160cm was observed on 2025-10-23 during a large flood event. The discharge estimate at this stage (~17.5 m³/s) carries higher uncertainty than rated values. A gauging at or near this flow level would substantially improve confidence in the upper end of the rating curve.

**October 2025 runoff coefficient >1:**
October 2025 shows a monthly runoff coefficient slightly exceeding 1.0. This is attributed to a combination of timing effects (antecedent catchment storage, flashy response to late-month events) and upper-end rating curve extrapolation uncertainty. The rain gauge record is complete for this period and discharge flags are clean. This is not considered a data quality issue.

**Rain gauge gaps (Hecate gauge):**
The rain_hecate gauge at 300m ASL has intermittent data gaps throughout the record, most notably in 2016-2017 where coverage is very sparse. Water years with >10% rain gauge gaps should be treated as unreliable for runoff coefficient analysis. These are flagged in the annual runoff coefficient plot.

**SA sensor deployment gap:**
The ssn844_sa supplementary sensor was active from 2018-09-02 to 2024-05-09. Stage gaps and bad data periods outside this window are filled by spline interpolation only (up to 3 hours) or left as NA. The 2021 slow drift period (22 days) falls within the SA deployment window and is filled from ssn844_sa.

## Discharge Record Quality Flags

The `discharge_flag` column mirrors the stage QC flag:

| Flag | Meaning |
|:-----|:--------|
| `raw` | Measured stage, no gap-filling applied |
| `gf_sa` | Gap filled using SA sensor relationship |
| `gf_spline` | Gap filled using spline interpolation (baseflow) |
| `gf_spline_event` | Gap filled using spline interpolation (event conditions) |
| `replaced_sa` | Bad data period replaced using SA sensor relationship |
| `replaced_spline` | Bad data period replaced using spline interpolation |
| `unfilled` | Stage gap could not be filled — discharge is NA |
| `out_of_range` | Stage outside rated range — discharge is NA |

## Rainfall-Runoff Validation

Annual runoff coefficients were computed using the rain_hecate gauge at 300m ASL as the precipitation input. Water years with >10% NA discharge or significant rain gauge gaps are flagged as unreliable in the plots.

General observations:
- Water years with good data coverage show runoff coefficients broadly consistent with a wet coastal BC catchment
- The Hecate gauge at 300m ASL is the best available representation of catchment-average rainfall for this site
- Water years prior to ~2018 have sparse rain gauge coverage and runoff coefficients should be interpreted cautiously

## Recommended Next Steps

1. Collect gaugings above 100cm stage at loc_1, prioritising the 110-146cm range to improve confidence in the upper curve
2. Obtain at least one gauging near peak flow conditions (>150cm) to constrain the extrapolated upper end
3. Consider installing a replacement supplementary sensor given ssn844_sa was decommissioned in May 2024
4. Update Rating 2 when sufficient new high-flow gaugings are available

## File Outputs

| File | Description |
|:-----|:------------|
| `ssn844_discharge_RC2.csv` | RC2 discharge, 2017-07-13 onward, 5-minute resolution |
| `ssn844_discharge_combined.csv` | RC2 discharge (same as above; RC1 not recomputed) |
| `ssn844_rating_curve_lookup_combined.csv` | Rating 1 (v1) + Rating 2 (extended to 160cm) |
| `ssn844_RC2_rating_curve_v2.csv` | Rating 2 extended lookup table with CI |

---
*Pipeline: v2 | Author: [your name]*
