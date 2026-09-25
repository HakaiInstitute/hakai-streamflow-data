# SSN703 RC5 — Can PT3 and PT4 Gaugings Be Combined? (Plain-Language Summary)

## Background

The current rating curve for SSN703 at loc_3 (RC5) is built on stage data from
sensor PT4 (ssn703_d). PT4 has only 19 gaugings, which makes RC5 sparse —
especially at high flows. The previous sensor, PT3 (ssn703_c) at loc_2, has 132
gaugings collected over 2019–2023.

The question we set out to answer: **can we shift the PT3 gaugings onto the PT4
datum and combine the two sets to build a better-constrained RC5?**
  
  If this worked, it would roughly seven-fold the data available to fit RC5 and
greatly improve confidence, particularly at the high end.

## What We Tested

We attempted to bring the two sets of gaugings onto a common datum and check
whether they then described the same stage–discharge relationship. We tried two
different stage offsets:
  
  1. **A best-fit offset (+1.5 cm)** — found by trial and error, searching for the
shift that minimised the overall mismatch between the two datasets.

2. **The physically-measured datum offset (−7 cm)** — derived from the period
(2021-09-02 to 2023-06-01) when PT3 and PT4 were running simultaneously and
measuring the same water surface. PT4 reads 7 cm higher than PT3.

For each offset, we shifted the PT3 gaugings, overlaid them on the PT4 gaugings,
and checked whether the mismatch between the two was consistent across all flows
or whether it changed with water level.

We also ran the test in **both directions** (shifting PT3 onto PT4, and PT4 onto
                                             PT3) to confirm the result did not depend on which sensor we chose as the
reference. As expected, the outcome was the same either way — a datum shift is
just a horizontal move of points along the stage axis, so the relationship
between the two datasets is identical regardless of direction. Flipping the
direction mirrors the residual plot but preserves its shape.

## What We Found

**Neither offset worked.** In both cases, once the gaugings were aligned on a
common datum, the mismatch between PT3 and PT4 **changed systematically with
water level** — they agreed at some flows but diverged at others.

This is the key diagnostic. A simple datum offset shifts every point up or down
by the same fixed amount. If a single offset truly reconciled the two sensors,
the leftover mismatch would be flat across all flows. Instead, the mismatch
followed a clear stage-dependent trend, and the divergence grew worse at high
flow (above roughly 130 cm).

A mismatch that changes with stage cannot be fixed by any single offset. It
means the two locations have **genuinely different stage–discharge
relationships** — different curve shapes, not just different sensor heights.

## Why the Two Locations Differ Hydraulically

Although PT3 and PT4 are only about a metre apart, they sit in hydraulically
distinct positions:
  
  - **PT4 is in a deeper, more steeply-banked part of the channel.** Because the
banks are steeper, the channel fills more slowly as water rises — so PT4
carries less flow for a given stage than PT3 does. This explains the
mid-range divergence.

- **PT3 appears to start spilling over its banks around 120–130 cm.** Above this
level, additional flow spreads sideways across the bank rather than rising
vertically. This makes the two sensors diverge even more sharply at high
water, which matches the steepening of the mismatch we observed above ~130 cm.

The data and the field setting tell a consistent story: a steeper-banked
confined section (PT4) next to a shallower section that overbanks at high flow
(PT3). Even a metre of lateral distance is enough to cross from one hydraulic
regime into another, which is why proximity does not imply the two sensors share
a rating.

## Decision

**RC5 is kept as a PT4-only curve.** Combining the PT3 gaugings would have pulled
the high end of the curve in the wrong direction — precisely where PT4 data is
thinnest and where the two locations differ most. A denser but biased curve is
worse than a sparse but correct one.

## Consequence and Recommendation

RC5 remains **provisional**, resting on 19 PT4 gaugings. The pooling test
confirms there is no shortcut to densifying it using PT3 data.

The priority for future fieldwork is collecting more gaugings at loc_3,
**especially above 130 cm**, since that is both where the PT4 data is sparsest
and where PT3 data is least able to stand in due to the overbank divergence.

## Methods Note

- Gaugings filtered to `rating_curve_period == "RC2"` (PT3) and `"RC3"` (PT4),
with `Final_rating_curve == "Y"` to use only the gaugings retained in the
production curves.
- Reference relationships fit as power laws in log space.
- Residuals assessed as log(observed Q) − log(predicted Q) against stage, with a
LOESS smoother to reveal any stage-dependent trend.
- Test script: `check_7cm_offset_703.R`
- Earlier exploratory grid search: `explore_loc2_loc3_offset_703.R`