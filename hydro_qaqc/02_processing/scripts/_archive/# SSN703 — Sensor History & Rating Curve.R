# SSN703 — Sensor History & Rating Curve Investigation: Summary

*A plain-language recap of this working thread — what was found, what was fixed, and what's still open.*

---

## How this started

The original ask was simple: build a plot layering all the SSN703 stage sensors together, with markers for where each sensor's deployment period ends and shading for which rating curve applies when. Everything below grew out of things that plot — and the questions it prompted — surfaced along the way.

---

## ✅ Done and solid — you can stop worrying about these

### 1. The sensor history plot itself
A static and an interactive (dygraphs) version were built, showing all SSN703 sensors overlaid, with sensor-end markers, rating curve period shading, and confirmed bad-data bands. Two real data issues were caught in the process:
- `ssn703_d` was rendering in the wrong colour due to a `site_id` naming mismatch (flagged, not yet fixed on your end).
- A large stage spike at the `ssn703_c`→`ssn703_d` handoff turned out to sit inside the already-known bad-data window — not a new problem.

### 2. Two confirmed-bad-data dates, now written into `sensor_registry`
- `ssn703_a`: unreliable from **2018-02-09**.
- `ssn703_c`: bad data now correctly extends to **2023-09-14** (previously understated as ending 2023-08-03).

### 3. A real provenance bug in the gauging table — found and fixed
This was the most concrete, valuable piece of work in the whole thread. For gaugings between **2018-09-14 and 2019-02-09**, the gauging table had been carrying `ssn703_c` (loc_2) stage values — because that's what the database associated with those events. But your former colleague had actually built the RC1 rating curve using `ssn703_b` (loc_1) stage exclusively for that window, and explicitly documented excluding `ssn703_c`. So the table had the *wrong sensor's stage* sitting next to the *right discharge* for ~28 historical events.

**What was done about it:**
- Confirmed the mismatch directly against her original historical gauging table.
- Recovered her actual `ssn703_b` stage values for those 28 events (with a Q-value cross-check to make sure timestamps were matched correctly, not just assumed).
- Applied the correction, re-labeled those events back to `RC1` (where they actually belong), and flagged them with a traceable `stage_source = "historical_ssn703b_recovered"`.
- Explicitly decided **not** to retroactively alter the historical published discharge record — this was a correction to the gauging table's internal consistency, not a revision of what was published.
- Promoted the corrected table to be the live `ssn703_gaugings_prepped.csv`, with the pre-fix version backed up separately.

**This is genuinely finished.** Nothing further needs to happen here unless the raw gauging data itself changes.

---

## 🟡 Investigated, but inconclusive — worth knowing about, not worth re-litigating

### Is there a genuine hydraulic control difference between loc_1 and loc_2?

This was an attempt to explain *why* stage differs between locations — is it just a fixed elevation/datum offset (same control, different sensor height), or a real difference in the channel/control shape?

Several statistical tests were run (power-law exponent comparison, different time windows, robustness checks). **The results contradicted each other depending on which window of data was used** — significant in one window, not significant in wider or shifted windows. A robustness check traced this to the result being driven by a small cluster of 2019 gaugings that didn't hold up once more data was included.

**Conclusion: this specific statistical approach was not able to reliably answer the question.** That's a real finding in itself — not a failure, just an honest limit of what ~30-70 discrete gaugings and a 4-5 parameter nonlinear model can pin down. This shouldn't be written up as "confirmed" in either direction.

---

## 📌 What's actually true and reliable, independent of the shaky statistics

- **There is a real, measurable stage difference between loc_1 and loc_2**, established the most trustworthy way possible: directly comparing her recovered `ssn703_b` readings against `ssn703_c` readings for the *same real discharge events*. Roughly **14cm at baseflow, growing to ~24cm at flood peaks.**
- **Discharge (not stage) is computed independently at each location** from its own gaugings. A stage difference between locations does not automatically mean a discharge discontinuity — that only follows if the individual rating curves themselves are poorly fit, which hasn't been shown.
- **The historical published discharge record (through Feb 2019) is untouched and doesn't need to be revisited** — that was a deliberate decision, not an oversight.

---

## 🔲 Still open — the one thing worth doing next, if/when you pick this back up

**Does the discharge record itself show a discontinuity at the loc_1→loc_2 boundary, independent of what's going on with stage?**

This is a different, more direct question than the exponent tests attempted, and it's better-suited to the data you actually have: `ssn703_b` and `ssn703_c` ran **concurrently for 2.5 years** (Sept 2018–Mar 2021), continuously — not just at the ~70 discrete gauging moments. The plan (not yet started) is:
1. Run RC1's curve against `ssn703_b`'s continuous stage, and RC2's curve against `ssn703_c`'s continuous stage, over that whole overlap window.
2. Compare the two resulting discharge series directly (Bland-Altman style — bias, limits of agreement, checked for whether the disagreement scales with flow).

This sidesteps the fragile `h0` extrapolation problem that made the exponent tests unstable, and answers the question that actually matters operationally — not "do the curve shapes match," but "does the number a data user would actually download show a jump."

---

## Separately — still fully open, not started

**The loc_2→loc_3 (RC2/RC3) gap**, Dec 2022–Sept 2023: no gaugings bridge this window at all. The plan there (also Bland-Altman, using the ~2-year `ssn703_c`/`ssn703_d` overlap from 2021-2023) hasn't been started.

---

*This document reflects the state of the investigation as of this thread. If you pick this back up later, this is the place to start.*