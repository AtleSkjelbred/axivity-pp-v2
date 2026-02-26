# Axivity-PP v2 - Variable Documentation

## Pipeline Overview

The pipeline processes epoch-level accelerometer CSV files. For each subject, it:
1. Detects the epoch rate from the first two timestamps
2. Filters the data (trims inactive ends, recodes activities, removes bad days)
3. Splits the data into days at midnight boundaries
4. Optionally maps work/other time shifts to the data
5. Computes variables at multiple aggregation levels

Results are written to separate CSV files, each controlled by a config toggle. Missing data is represented according to the `na_rep` config option (default: empty string; common alternatives: `'.'`, `'NA'`). All epoch counts below refer to the number of rows in the processed dataframe matching a given condition within a time segment.

---

## 1. Post Process Data (`base_variables: True`)

Subject metadata and recording-level summaries.

| Column | Description |
|--------|-------------|
| `subject_id` | Subject identifier from the input CSV |
| `epoch_per_min` | Epochs per minute (e.g., 1 for 60s epochs, 2 for 30s epochs) |
| `epoch_per_day` | Epochs per full day (epm * 60 * 24) |
| `total_epochs` | Total number of epochs in the raw input file |
| `recording_start` | First timestamp in the recording (dd.mm.yyyy HH:MM) |
| `recording_end` | Last timestamp in the recording (dd.mm.yyyy HH:MM) |
| `epochs_removed` | Epochs trimmed from start/end during inactive-end removal |
| `total_days_before_filter` | Number of days before day quality filtering |
| `days_removed` | Number of days removed by day quality filtering |
| `total_nw_code_{c}` | Non-wear percentage for code c across all valid days (if `nw_variables`) |
| `total_days` | Total measurement time in fractional days (total valid epochs / epd) |
| `wk_days` | Weekday measurement time in fractional days |
| `wknd_days` | Weekend measurement time in fractional days |
| `nr_ot` | Number of valid work/other time shifts (if `ot_variables`) |

---

## 2. Average Data (`average_variables: True`)

Per-day averages across all valid days.

### Context

| Column | Description |
|--------|-------------|
| `subject_id` | Subject identifier |
| `total_days` | Total fractional days |
| `wk_days` | Weekday fractional days |
| `wknd_days` | Weekend fractional days |

### Activity Variables

Three parallel sets of activity variables are computed using the same logic but on different columns and code sets:

| Set | Column | Codes (default) |
|-----|--------|-----------------|
| **act** | `label` | walking(1), running(2), standing(6), sitting(7), lying(8), cycling(13) |
| **ai** | `ai_column` | Active(A), Inactive(I) |
| **walk** | `walking_intensity_prediction` | slow(101), moderate(102), fast(103), stairs(104) |

The `ai_column` is derived during filtering: sitting and lying = Inactive (I), all other codes = Active (A).

| Column | Unit | Formula |
|--------|------|---------|
| `avg_{name}_min` | min/day | (sum of daily totals / total_days) / epm |
| `avg_{name}_pct` | %/day | (sum of daily totals / total_days) / epd * 100 |

Where `{name}` is the code's display name from `code_name` in config (e.g., `walking`, `sitting`, `active`, `inactive`, `slow`, etc.).

### AIT Variables

| Column | Unit | Formula |
|--------|------|---------|
| `avg_ait` | count/day | sum of daily totals / total_days |

### Bout Variables

| Column | Unit | Formula |
|--------|------|---------|
| `avg_{name}_bout_c{n}` | count/day | sum across days / total_days |

Where `{name}` is the activity name and `{n}` is the bout category (1-5).

### Weekday / Weekend Averages (`week_wknd_variables: True`)

Same as above but partitioned by weekday (Mon-Fri) and weekend (Sat-Sun). If there are no weekdays (or no weekends), the corresponding values are `None`.

| Column | Unit | Formula |
|--------|------|---------|
| `avg_wk_{name}_min` | min/day | (sum of weekday totals / wk_days) / epm |
| `avg_wk_{name}_pct` | %/day | (sum of weekday totals / wk_days) / epd * 100 |
| `avg_wknd_{name}_min` | min/day | (sum of weekend totals / wknd_days) / epm |
| `avg_wknd_{name}_pct` | %/day | (sum of weekend totals / wknd_days) / epd * 100 |
| `avg_wk_ait` | count/day | sum of weekday totals / wk_days |
| `avg_wknd_ait` | count/day | sum of weekend totals / wknd_days |
| `avg_wk_{name}_bout_c{n}` | count/day | sum of weekday values / wk_days |
| `avg_wknd_{name}_bout_c{n}` | count/day | sum of weekend values / wknd_days |

---

## 3. Daily Data (`daily_variables: True`)

Per-day breakdown of all enabled variables. The output format depends on `long_format`:

- **Wide format** (`long_format: False`, default): one row per subject, columns prefixed with `day{d}_`
- **Long format** (`long_format: True`): one row per (subject, day), no prefix

### Day Information

| Wide column | Long column | Description |
|-------------|-------------|-------------|
| `subject_id` | `subject_id` | Subject identifier |
| `day{d}_nr` | `day_nr` | Day sequence number |
| `day{d}_date` | `date` | Date string (YYYY-MM-DD) |
| `day{d}_wkday_nr` | `wkday_nr` | Weekday number (Monday=1 ... Sunday=7) |
| `day{d}_wkday_str` | `wkday_str` | Weekday name (e.g., "Monday") |
| `day{d}_length_min` | `length_min` | Day length in epochs |
| `day{d}_length_pct` | `length_pct` | Day length as percentage of a full day (length / epd * 100) |

### Non-Wear

| Wide column | Long column | Unit | Formula |
|-------------|-------------|------|---------|
| `day{d}_nw_code_{c}` | `nw_code_{c}` | % | Epochs with code c on day d / day d length * 100 |

### Activity Counts

| Wide column | Long column | Unit | Description |
|-------------|-------------|------|-------------|
| `day{d}_total_{name}` | `total_{name}` | epochs | Total epoch count for this code on day d |
| `day{d}_other_{name}` | `other_{name}` | epochs | Epoch count during OT shifts (if `ot_variables`) |
| `day{d}_normal_{name}` | `normal_{name}` | epochs | Epoch count outside OT shifts (if `ot_variables`) |

### AIT

| Wide column | Long column | Unit |
|-------------|-------------|------|
| `day{d}_total_ait` | `total_ait` | count |
| `day{d}_other_ait` | `other_ait` | count (during OT, if `ot_variables`) |
| `day{d}_normal_ait` | `normal_ait` | count (outside OT, if `ot_variables`) |

### Bouts

| Wide column | Long column | Unit |
|-------------|-------------|------|
| `day{d}_{name}_total_bouts_c{n}` | `{name}_total_bouts_c{n}` | count of bouts in category n |
| `day{d}_{name}_other_bouts_c{n}` | `{name}_other_bouts_c{n}` | count during OT (if `ot_variables`) |
| `day{d}_{name}_normal_bouts_c{n}` | `{name}_normal_bouts_c{n}` | count outside OT (if `ot_variables`) |

---

## 4. OT Data (`ot_variables: True`)

Per-shift variables for all raw work shifts. No filtering or merging is applied — every shift from the work-times file that passes basic validation (valid timestamps, within data range, no overlaps) is included.

The output format depends on `long_format`:

- **Wide format** (`long_format: False`, default): one row per subject, columns prefixed with `ot{s}_`
- **Long format** (`long_format: True`): one row per shift, no prefix, with a `type` column (`ot`)

### Row Information

| Wide column | Long column | Description |
|-------------|-------------|-------------|
| `subject_id` | `subject_id` | Subject identifier |
| — | `type` | Row type: `ot` (long format only) |
| `ot{s}_nr` | `nr` | Shift sequence number |
| `ot{s}_start_datetime` | `start_datetime` | Start time, inclusive (dd.mm.yyyy HH:MM) |
| `ot{s}_end_datetime` | `end_datetime` | End time, inclusive — timestamp of the last epoch in the shift (dd.mm.yyyy HH:MM) |
| `ot{s}_start_wkday_nr` | `start_wkday_nr` | Weekday number (Monday=1 ... Sunday=7) |
| `ot{s}_start_wkday_str` | `start_wkday_str` | Weekday name |
| `ot{s}_epochs` | `epochs` | Length in epochs |
| `ot{s}_min` | `min` | Length in minutes (epochs / epm) |

### Activity Counts

For each code in the ai, act, and walk sets:

| Wide column | Long column | Unit | Formula |
|-------------|-------------|------|---------|
| `ot{s}_{name}_min` | `{name}_min` | minutes | epoch count / epm |
| `ot{s}_{name}_pct` | `{name}_pct` | % | epoch count / shift epochs * 100 |

### Walking Intensity

Percentage of total walking within the shift:

| Wide column | Long column | Unit | Formula |
|-------------|-------------|------|---------|
| `ot{s}_walk{name}_min` | `walk{name}_min` | minutes | epoch count / epm |
| `ot{s}_walk{name}_pct` | `walk{name}_pct` | % | epoch count / total walking epochs * 100 |

### Non-Wear

| Wide column | Long column | Unit | Formula |
|-------------|-------------|------|---------|
| `ot{s}_nw_code_{c}_pct` | `nw_code_{c}_pct` | % | epoch count / shift epochs * 100 |

### AIT and Bouts

| Wide column | Long column | Unit |
|-------------|-------------|------|
| `ot{s}_ait` | `ait` | AIT count (if `ait_variables`) |
| `ot{s}_{name}_bout_c{n}` | `{name}_bout_c{n}` | Bout count in category n (if `bout_variables`) |

---

## 5. Between OT Data (`between_ot_variables: True`)

Modified shift and between-period variables. Unlike the raw OT data, this file applies restrictions:
- Shifts shorter than `min_shift_minutes` (default 60) are excluded as standalone shifts
- Consecutive adjacent shifts are merged into one
- Short (excluded) shifts within between-ot sections are carved out

The file contains two types of entries: **modified shifts** (`mod_ot`) and **between-ot periods** (`between`).

The output format depends on `long_format`:

- **Wide format** (`long_format: False`, default): one row per subject, columns prefixed with `mod_ot{s}_` (modified shifts) and `between{s}_` (between periods)
- **Long format** (`long_format: True`): one row per entry, no prefix, with a `type` column (`mod_ot` or `between`)

### Row Information

| Wide column | Long column | Description |
|-------------|-------------|-------------|
| `subject_id` | `subject_id` | Subject identifier |
| — | `type` | Row type: `mod_ot` or `between` (long format only) |
| `mod_ot{s}_nr` / `between{s}_nr` | `nr` | Sequence number |
| `mod_ot{s}_start_datetime` / `between{s}_start_datetime` | `start_datetime` | Start time, inclusive (dd.mm.yyyy HH:MM) |
| `mod_ot{s}_end_datetime` / `between{s}_end_datetime` | `end_datetime` | End time, inclusive — timestamp of the last epoch in the section (dd.mm.yyyy HH:MM) |
| `mod_ot{s}_start_wkday_nr` / `between{s}_start_wkday_nr` | `start_wkday_nr` | Weekday number (Monday=1 ... Sunday=7) |
| `mod_ot{s}_start_wkday_str` / `between{s}_start_wkday_str` | `start_wkday_str` | Weekday name |
| `mod_ot{s}_epochs` / `between{s}_epochs` | `epochs` | Length in epochs |
| `mod_ot{s}_min` / `between{s}_min` | `min` | Length in minutes (epochs / epm) |

### Activity Counts

For each code in the ai, act, and walk sets:

| Wide column | Long column | Unit | Formula |
|-------------|-------------|------|---------|
| `{prefix}_{name}_min` | `{name}_min` | minutes | epoch count / epm |
| `{prefix}_{name}_pct` | `{name}_pct` | % | epoch count / section epochs * 100 |

### Walking Intensity

Percentage of total walking within the section:

| Wide column | Long column | Unit | Formula |
|-------------|-------------|------|---------|
| `{prefix}_walk{name}_min` | `walk{name}_min` | minutes | epoch count / epm |
| `{prefix}_walk{name}_pct` | `walk{name}_pct` | % | epoch count / total walking epochs * 100 |

### Non-Wear

| Wide column | Long column | Unit | Formula |
|-------------|-------------|------|---------|
| `{prefix}_nw_code_{c}_pct` | `nw_code_{c}_pct` | % | epoch count / section epochs * 100 |

### AIT and Bouts

| Wide column | Long column | Unit |
|-------------|-------------|------|
| `{prefix}_ait` | `ait` | AIT count (if `ait_variables`) |
| `{prefix}_{name}_bout_c{n}` | `{name}_bout_c{n}` | Bout count in category n (if `bout_variables`) |

Where `{prefix}` is `mod_ot{s}` or `between{s}` in wide format.

### Between-OT Sections

The between-ot section following each modified shift extends to the start of the next modified shift, capped at 24 hours and at the end of the data. Short (excluded) shifts within between sections are carved out, resulting in potentially multiple sub-ranges. Variables are summed across sub-ranges.

---

## How Activity Counts Are Computed

For a given day and code, the **total** is the number of epochs in that day where the column equals the code.

When OT is enabled, the **other** count sums the epochs matching the code within all OT shift segments that overlap with that day. This includes:
- Shifts starting on that day (clamped to the day's end boundary)
- Overnight shifts from the previous day that extend into the current day (clamped to the day's end boundary)

The **normal** count is: total - other.

---

## How AIT Is Computed

An AIT is counted as the number of distinct contiguous groups of Active ('A') epochs within a time segment. Each group represents one period of activity.

1. Extract all indices where `ai_column == 'A'` within the segment
2. Group consecutive indices into contiguous runs
3. Count the number of runs

For OT splits: the same logic is applied to each OT shift segment overlapping the day (with boundary clamping). Normal = total - other.

---

## How Bouts Are Detected

A bout is a sustained period of a single activity code, with noise tolerance for brief interruptions. Bouts are computed for the codes defined in `bout_codes` (default: walking, running, standing, sitting, lying).

The algorithm scans through the activity column within a segment [start, end):

1. Find the first valid bout code in the segment
2. For each subsequent epoch:
   - If it matches the current code: extend the bout (length += 1)
   - If it does not match: check if this is tolerable noise or a bout break

**Noise tolerance rules** (checked when a non-matching epoch is encountered):
- **Short bouts** (< `length_threshold` seconds): tolerate a gap of 1 epoch
- **Long bouts** (>= `length_threshold` seconds): tolerate a gap of up to 2 epochs, provided the cumulative noise ratio stays below `noise_threshold`

If the interruption is tolerated, the epoch is absorbed into the current bout as noise. If not, the current bout is finalized and a new bout begins.

### Bout Categories

After detection, raw bout lengths (in epochs) are sorted into duration categories. The category boundaries are defined in seconds in config and converted to epochs:

`epoch_threshold = seconds_threshold * (epm / 60)`

Sitting and lying use `i_cat` (inactive categories), all others use `a_cat` (active categories):

| Category | Duration range (default) |
|----------|---------------------------------|
| c1 | 60 - 300 seconds (1 - 5 min) |
| c2 | 301 - 600 seconds (5 - 10 min) |
| c3 | 601 - 1200 seconds (10 - 20 min) |
| c4 | 1201 - 3600 seconds (20 - 60 min) |
| c5 | 3601+ seconds (60+ min) |

Each category stores the **count** of bouts falling within that duration range.

---

## Pre-Processing Steps

These steps modify the data before any variable computation:

### 1. Inactive End Removal

Scans inward from both ends of the recording. For each end, finds the first non-inactive epoch and checks whether the window beyond it exceeds the inactivity threshold. Trims the data to this point.

Applied for:
- Non-wear code 4 (no sensors): window = `nw_ends_min` min, threshold = `nw_ends_pct`
- Lying: window = `bug_ends_min` min, threshold = `bug_ends_pct`
- Sitting: window = `bug_ends_min` min, threshold = `bug_ends_pct`

### 2. Activity Recoding

Applied in order:

| Step | Action |
|------|--------|
| `remove_stairs` | Recode stairs to walking; assign walk intensity code `stair_walk_code` |
| `remove_bending` | Recode bending to standing |
| `remove_shuffling` | Recode shuffling to walking if between two walking epochs, otherwise to standing |
| `merge_cyc_codes` | Recode all cycling variants to cycling |
| `adjust_cyc_interval` | Replace cycling bouts of `min_cyc_epochs` or fewer epochs with the surrounding activity code |
| `code_remap` | Custom code-to-code remapping (applied last) |

### 3. AI Column Creation

A derived column (`ai_column`) is created: sitting and lying are mapped to 'I' (inactive), all other codes to 'A' (active). Only created if `ai_variables` or `ait_variables` is enabled.

### 4. Day Filtering

Days are removed from analysis if they exceed any of these thresholds:

| Condition | Threshold (default) |
|-----------|---------------------|
| Non-wear code 4 | > `nw_days_pct` of the day |
| Lying | > `bug_days_pct` of the day (if `bug_lying`) |
| Sitting | > `bug_days_pct` of the day (if `bug_sitting`) |
| Standing | > `bug_days_pct` of the day (if `bug_standing`) |
| Partial day (optional) | Day length < epd (if `remove_partial_days`) |

After removal, day keys are renumbered to contiguous 1..N.
