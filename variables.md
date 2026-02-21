# Axivity-PP v2 - Variable Documentation

## Pipeline Overview

The pipeline processes epoch-level accelerometer CSV files. For each subject, it:
1. Detects the epoch rate from the first two timestamps
2. Filters the data (trims inactive ends, recodes activities, removes bad days)
3. Splits the data into days at midnight boundaries
4. Optionally maps work/other time shifts to the data
5. Computes variables at multiple aggregation levels

All epoch counts below refer to the number of rows in the processed dataframe matching a given condition within a time segment.

---

## Subject Metadata

| Column | Description |
|--------|-------------|
| `subject_id` | Subject identifier from the input CSV |
| `epoch per min` | Epochs per minute (e.g., 1 for 60s epochs, 2 for 30s epochs) |
| `epoch per day` | Epochs per full day (epm * 60 * 24) |
| `epochs_removed` | Number of epochs trimmed from the start/end during inactive-end removal |

---

## Non-Wear Variables

Computed from the non-wear sensor column (`snt_prediction` by default).

### Total (across all valid days)

| Column | Unit | Formula |
|--------|------|---------|
| `total_nw_code_1` | % | Epochs with code 1 (both sensors worn) / total epochs * 100 |
| `total_nw_code_2` | % | Epochs with code 2 (back sensor only) / total epochs * 100 |
| `total_nw_code_3` | % | Epochs with code 3 (thigh sensor only) / total epochs * 100 |
| `total_nw_code_4` | % | Epochs with code 4 (no sensors worn) / total epochs * 100 |

### Daily

| Column | Unit | Formula |
|--------|------|---------|
| `day{d}_nw_code_{c}` | % | Epochs with code c on day d / day d length in epochs * 100 |

---

## Day Information

| Column | Description |
|--------|-------------|
| `total_days` | Total measurement time in fractional days (total epochs / epd) |
| `wk_days` | Weekday measurement time in fractional days |
| `wknd_days` | Weekend measurement time in fractional days |
| `nr_ot` | Number of valid work/other time shifts (only if OT enabled) |

### Per-Day

| Column | Description |
|--------|-------------|
| `day{d}_nr` | Day sequence number (1, 2, 3...) |
| `day{d}_date` | Date string (YYYY-MM-DD) |
| `day{d}_wkday_nr` | Weekday number (Monday=1 ... Sunday=7) |
| `day{d}_wkday_str` | Weekday name (e.g., "Monday") |
| `day{d}_length_min` | Day length in epochs (note: column name says min but value is epochs) |
| `day{d}_length_pct` | Day length as percentage of a full day (length / epd * 100) |

---

## Activity Variables

Three parallel sets of activity variables are computed using the same logic but on different columns and code sets:

| Set | Column | Codes (default config) |
|-----|--------|------------------------|
| **act** | `label` | walking(1), running(2), standing(6), sitting(7), lying(8), cycling(13) |
| **ai** | `ai_column` | Active(A), Inactive(I) |
| **walk** | `walking_intensity_prediction` | 101, 102, 103, 104 |

The `ai_column` is derived during filtering: sitting and lying are classified as Inactive (I), all other activity codes as Active (A).

### How activity counts are computed

For a given day and code, the **total** is the number of epochs in that day where the column equals the code.

When other time (OT) is enabled, the **other** count sums the epochs matching the code within all OT shift segments that overlap with that day. This includes:
- Shifts starting on that day (clamped to the day's end boundary)
- Overnight shifts from the previous day that extend into the current day (clamped to the day's end boundary)

The **normal** count is: total - other.

### Daily output

| Column | Unit | Description |
|--------|------|-------------|
| `day{d}_total_{name}` | epochs | Total epoch count for this code on day d |
| `day{d}_other_{name}` | epochs | Epoch count during OT shifts on day d |
| `day{d}_normal_{name}` | epochs | Epoch count outside OT shifts on day d |

Where `{name}` is the code's display name from `code_name` in config (e.g., `walking`, `sitting`, `active`, `inactive`, `101`, etc.).

### Average output

Averages are computed as: sum of epoch counts across all valid days, divided by total fractional measurement days.

| Column | Unit | Formula |
|--------|------|---------|
| `avg_{name}_min` | min/day | (sum of daily totals / total_days) / epm |
| `avg_{name}_pct` | %/day | (sum of daily totals / total_days) / epd * 100 |

### Weekday / Weekend output

Same as average but partitioned by weekday (Mon-Fri) and weekend (Sat-Sun). The denominator is the fractional days for that partition.

| Column | Unit | Formula |
|--------|------|---------|
| `avg_wk_{name}_min` | min/day | (sum of weekday totals / wk_days) / epm |
| `avg_wk_{name}_pct` | %/day | (sum of weekday totals / wk_days) / epd * 100 |
| `avg_wknd_{name}_min` | min/day | (sum of weekend totals / wknd_days) / epm |
| `avg_wknd_{name}_pct` | %/day | (sum of weekend totals / wknd_days) / epd * 100 |

If there are no weekdays (or no weekends), the corresponding values are `None`.

---

## Active-Inactive Transition (AIT) Variables

An AIT is counted as the number of distinct contiguous groups of Active ('A') epochs within a time segment. Each group represents one period of activity, and the count equals the number of times the subject transitioned from active to inactive (or became active, for the final group if it extends to the segment boundary).

### How AIT is computed

1. Extract all indices where `ai_column == 'A'` within the segment
2. Group consecutive indices into contiguous runs
3. Count the number of runs

For OT: the same logic is applied to each OT shift segment overlapping the day (with boundary clamping). Normal = total - other.

### Daily output

| Column | Unit |
|--------|------|
| `day{d}_total_ait` | count |
| `day{d}_other_ait` | count (during OT) |
| `day{d}_normal_ait` | count (outside OT) |

### Average output

| Column | Unit | Formula |
|--------|------|---------|
| `avg_ait` | count/day | sum of daily totals / total_days |
| `avg_wk_ait` | count/day | sum of weekday totals / wk_days |
| `avg_wknd_ait` | count/day | sum of weekend totals / wknd_days |

---

## Bout Variables

A bout is a sustained period of a single activity code, with noise tolerance for brief interruptions. Bouts are computed for the codes defined in `bout_codes`: walking(1), running(2), standing(6), sitting(7), lying(8).

### How bouts are detected

The algorithm scans through the activity column within a segment [start, end):

1. Find the first valid bout code in the segment
2. For each subsequent epoch:
   - If it matches the current code: extend the bout (length += 1)
   - If it does not match: check if this is tolerable noise or a bout break

**Noise tolerance rules** (checked when a non-matching epoch is encountered):
- **Short bouts** (< 20 minutes): tolerate a gap of 1 epoch (the next matching epoch must be within 1 position)
- **Long bouts** (>= 20 minutes): tolerate a gap of up to 2 epochs, provided the cumulative noise ratio stays below 15%

If the interruption is tolerated, the epoch is absorbed into the current bout as noise. If not, the current bout is finalized and a new bout begins.

The `find_next` and `skip` helper functions search only within the segment boundary to avoid cross-day contamination.

### How bouts are categorized

After detection, raw bout lengths (in epochs) are sorted into duration categories. The category boundaries are defined in seconds in config and converted to epochs:

`epoch_threshold = seconds_threshold * (epm / 60)`

Sitting and lying use `i_cat` (inactive categories), all others use `a_cat` (active categories):

| Category | Duration range (default config) |
|----------|---------------------------------|
| c1 | 60 - 300 seconds (1 - 5 min) |
| c2 | 301 - 600 seconds (5 - 10 min) |
| c3 | 601 - 1200 seconds (10 - 20 min) |
| c4 | 1201 - 3600 seconds (20 - 60 min) |
| c5 | 3601+ seconds (60+ min) |

Each category stores the **count** of bouts falling within that duration range.

### For OT

OT bout counts are computed by running the same detection algorithm on each OT shift segment overlapping the day, then summing element-wise across shifts. Normal = total - other (element-wise per category).

### Daily output

| Column | Unit |
|--------|------|
| `day{d}_{name}_total_bouts_c{n}` | count of bouts in category n |
| `day{d}_{name}_other_bouts_c{n}` | count during OT |
| `day{d}_{name}_normal_bouts_c{n}` | count outside OT |

Where `{name}` is the activity name (walking, running, standing, sitting, lying) and `{n}` is the category number (1-5).

### Average output

| Column | Unit | Formula |
|--------|------|---------|
| `avg_{name}_bout_c{n}` | count/day | sum across days / total_days |
| `avg_wk_{name}_bout_c{n}` | count/day | sum of weekday values / wk_days |
| `avg_wknd_{name}_bout_c{n}` | count/day | sum of weekend values / wknd_days |

---

## Other Time (OT) Per-Shift Variables

When OT is enabled, per-shift variables are output for each valid work time shift.

| Column | Description |
|--------|-------------|
| `ot{s}_nr` | Shift sequence number |
| `ot{s}_start_datetime` | Shift start (dd.mm.yyyy HH:MM), mapped to nearest epoch |
| `ot{s}_end_datetime` | Last epoch in shift (dd.mm.yyyy HH:MM) |
| `ot{s}_start_wkday_nr` | Weekday number of shift start (Monday=1 ... Sunday=7) |
| `ot{s}_start_wkday_str` | Weekday name of shift start |
| `ot{s}_length` | Shift length in epochs |
| `ot{s}_ait` | AIT count within the shift |

### Per-shift activity counts

For each code in the ai, act, and walk code sets:

| Column | Unit | Formula |
|--------|------|---------|
| `ot{s}_{name}_min` | minutes | Time spent in activity within the shift (epoch count / epm) |
| `ot{s}_{name}_pct` | % | Epoch count / shift length * 100 |

### Per-shift bout counts

| Column | Unit |
|--------|------|
| `ot{s}_{name}_bout_c{n}` | count of bouts in category n within the shift |

---

## Standalone Shift Pipeline Variables (main_ot.py)

`main_ot.py` outputs a separate set of per-shift and between-shift variables. Shifts shorter than `min_shift_minutes` (default 60) are excluded as standalone shifts. Consecutive adjacent shifts are merged. The between-shift section following each shift extends to the start of the next shift, capped at 24 hours. Short (excluded) shifts within between sections are carved out.

### Per-shift variables

| Column | Unit | Description |
|--------|------|-------------|
| `shift{s}_epochs` | epochs | Shift length in epochs |
| `shift{s}_min` | minutes | Shift length in minutes (epochs / epm) |
| `shift{s}_{name}_min` | minutes | Time spent in activity within the shift (epoch count / epm) |
| `shift{s}_{name}_pct` | % | Percentage of shift spent in activity (epoch count / shift epochs * 100) |
| `shift{s}_ait` | count | Active-inactive transitions within the shift |
| `shift{s}_{name}_bout_c{n}` | count | Bout count in duration category n within the shift |

### Between-shift variables

Between-shift sections may consist of multiple sub-ranges (when short excluded shifts are carved out).

| Column | Unit | Description |
|--------|------|-------------|
| `between{s}_epochs` | epochs | Total between-section length in epochs (sum of sub-ranges) |
| `between{s}_min` | minutes | Total between-section length in minutes |
| `between{s}_{name}_min` | minutes | Time spent in activity across all sub-ranges (epoch count / epm) |
| `between{s}_{name}_pct` | % | Percentage of between-section spent in activity |
| `between{s}_ait` | count | Active-inactive transitions summed across sub-ranges |
| `between{s}_{name}_bout_c{n}` | count | Bout count in duration category n summed across sub-ranges |

Where `{s}` is the shift number (1, 2, ...), `{name}` is the activity display name from `code_name`, and `{n}` is the bout duration category (1-5).

---

## Pre-Processing Steps

These steps modify the data before any variable computation:

### 1. Inactive End Removal

Scans inward from both ends of the recording. For each end, finds the first non-inactive epoch and checks whether the window beyond it exceeds the inactivity threshold. Trims the data to this point.

Applied for:
- Non-wear code 4 (no sensors): window = 45 min, threshold = 5%
- Lying (code 8): window = 45 min, threshold = 50%
- Sitting (code 7): window = 45 min, threshold = 50%

### 2. Activity Recoding

| Setting | Action |
|---------|--------|
| `remove_stairs` | Recode stairs (4,5) to walking (1); assign walk intensity code 104 |
| `remove_bending` | Recode bending (10) to standing (6) |
| `remove_shuffling` | Recode shuffling (3) to walking (1) if between two walking epochs, otherwise to standing (6) |
| `merge_cyc_codes` | Recode all cycling variants (13,14,130,140) to cycling (13) |
| `adjust_cyc_interval` | Replace cycling bouts of 3 or fewer epochs with the surrounding activity code |

### 3. AI Column Creation

A derived column (`ai_column`) is created: sitting(7) and lying(8) are mapped to 'I' (inactive), all other codes to 'A' (active).

### 4. Day Filtering

Days are removed from analysis if they exceed any of these thresholds:

| Condition | Threshold (default) |
|-----------|---------------------|
| Non-wear code 4 | > 50% of the day |
| Lying | > 80% of the day |
| Sitting | > 80% of the day |
| Standing | > 80% of the day |
| Partial day (optional) | Day length < epd |

After removal, day keys are renumbered to contiguous 1..N.
