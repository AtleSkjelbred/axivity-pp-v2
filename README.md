# Axivity Post-Processing Pipeline v2

Post-processing pipeline for epoch-level accelerometer data exported from Axivity sensors. For each subject it filters the data, segments it into days, optionally maps work shifts, and computes a wide range of activity variables.

There are two entry points:

- **`main.py`** — Full pipeline: end trimming, activity recoding, day segmentation, day quality filters, work-shift mapping, and variable calculation (daily, average, weekday/weekend, work/normal splits).
- **`main_ot.py`** — Standalone work-shift pipeline: processes the same input files but outputs per-shift and between-shift activity variables only.

Both scripts read configuration from `config.yaml` in the working directory. Results are written to `./results/`.

## Installation

### Requirements

- Python 3.9+
- [Anaconda](https://www.anaconda.com/) (recommended) or any Python environment manager

### Steps

1. Open a terminal (Anaconda Prompt on Windows)
2. Clone the repository:
   ```
   git clone https://github.com/AtleSkjelbred/axivity-pp-v2.git
   ```
3. Navigate to the project folder:
   ```
   cd axivity-pp-v2
   ```
4. Create and activate a Python environment:
   ```
   conda create --name axivity python=3.9 --no-default-packages -y
   conda activate axivity
   ```
5. Install dependencies:
   ```
   pip install pandas pyyaml matplotlib numpy
   ```

## Input Data

Place epoch-level CSV files in the `./data/` folder (or specify a path with `--data-folder`). Each CSV must contain at minimum:

| Column (default name) | Description |
|------------------------|-------------|
| `SID` | Subject/participant identifier |
| `timestamp` | Epoch timestamp (used to detect epoch rate and segment days) |
| `label` | Numeric activity prediction code from the classifier |
| `walking_intensity_prediction` | Walking intensity code (optional, needed if `walk_variables` is enabled) |
| `snt_prediction` | Non-wear sensor prediction code (optional, needed if `nw_variables` or `nw_ends`/`nw_days` are enabled) |

Column names are configurable in `config.yaml`.

### Work-times file

If work-shift analysis is enabled (`ot_run: True`), a separate CSV file with shift start/end times is required. Set the path in `config.yaml` under `ot_path`. The file format is auto-detected (date-time columns or numeric indices).

## Usage

Activate the environment first:
```
conda activate axivity
```

### Full pipeline

```
python main.py [--data-folder PATH]
```

If `--data-folder` is omitted, defaults to `./data/`.

### Work-shift only pipeline

```
python main_ot.py [--data-folder PATH]
```

### Output

Results are saved to `./results/` with a timestamp in the filename:

| File | Description |
|------|-------------|
| `post process data <timestamp>.csv` | Main output from `main.py` — all computed variables per subject |
| `other time qc <timestamp>.csv` | QC report for work-shift matching from `main.py` |
| `ot shift data <timestamp>.csv` | Per-shift output from `main_ot.py` |
| `ot shift qc <timestamp>.csv` | QC report from `main_ot.py` |
| `error_log <timestamp>.csv` | Files that failed processing (missing columns, bad timestamps, etc.) |
| `barcode/` | Activity barcode plots (only if `barcode_run: True`) |

## Configuration

All settings are in `config.yaml`. The file is commented — see it for full details. Key sections are summarised below.

### Column names

Map the column names in your input CSVs to what the pipeline expects. Change these if your files use different headers.

### Work-shift settings

| Setting | Description |
|---------|-------------|
| `ot_run` | Enable/disable work-shift analysis in `main.py` |
| `ot_path` | Path to the work-times CSV file |
| `ot_delimiter` | Delimiter of the work-times CSV (e.g. `;`) |
| `min_shift_minutes` | Shifts shorter than this (in minutes) are excluded as standalone shifts and do not interrupt between-shift sections. Default: 60 |
| `barcode_run` | Generate barcode activity plots |

### End trimming

Scans inward from both ends of a recording and trims epochs that are dominated by non-wear or a single posture (likely sensor malfunction or the sensor lying on a table). Controlled by:

- `nw_ends` / `bug_ends` — enable trimming for non-wear / buggy postures
- `*_ends_min` — window size in minutes
- `*_ends_pct` — fraction threshold (trim if the target code exceeds this fraction of the window)

### Day quality filters

Entire days are removed if they are dominated by non-wear or a single posture:

- `nw_days` / `bug_days` — enable day-level filtering
- `nw_days_pct` / `bug_days_pct` — fraction thresholds
- `bug_lying`, `bug_sitting`, `bug_standing` — which postures to check
- `min_days` — minimum valid days required to produce output for a subject
- `remove_partial_days` — optionally remove days shorter than 24 hours

### Activity code remapping

Before analysis, activity codes can be simplified:

| Setting | Effect |
|---------|--------|
| `remove_stairs` | Remap stair ascend/descend to walking |
| `remove_bending` | Remap bending to standing |
| `remove_shuffling` | Remap shuffling to walking (if between walks) or standing |
| `merge_cyc_codes` | Merge all cycling variants into a single cycling code |
| `adjust_cyc_interval` | Replace short cycling bouts (<=`min_cyc_epochs`) with surrounding activity |

### Output variable groups

Toggle which categories of variables are included in the output:

| Setting | Variables |
|---------|-----------|
| `act_variables` | Activity counts and percentages per activity code |
| `ai_variables` | Active/inactive time |
| `walk_variables` | Walking intensity counts |
| `ait_variables` | Active-inactive transition counts |
| `bout_variables` | Bout counts per duration category |
| `nw_variables` | Non-wear percentages |
| `daily_variables` | Per-day breakdown |
| `average_variables` | Averages across all valid days |
| `week_wknd_variables` | Separate weekday/weekend averages |
| `ot_variables` | Work-shift vs non-work-shift breakdown |

### Bout duration categories

Bouts are classified into duration bins defined in seconds (`i_cat` for inactive postures, `a_cat` for active). Default categories:

| Category | Duration |
|----------|----------|
| c1 | 1–5 min |
| c2 | 5–10 min |
| c3 | 10–20 min |
| c4 | 20–60 min |
| c5 | 60+ min |

### Activity codes and display names

The `codes` section maps activity names to the numeric codes used by the classifier. The `code_name` section maps codes to human-readable names used as column suffixes in the output.

## Processing Steps

1. **Epoch detection** — The epoch rate is auto-detected from the first two timestamps.
2. **End trimming** — Non-wear and buggy posture epochs are trimmed from recording start/end.
3. **Activity recoding** — Stairs, bending, shuffling, and cycling codes are remapped. An active/inactive (AI) column is derived (sitting and lying = Inactive, everything else = Active).
4. **Day segmentation** — The recording is split into days at midnight boundaries.
5. **Day filtering** — Days failing quality checks are removed; remaining days are renumbered 1..N.
6. **Work-shift mapping** — Shift times are matched to accelerometer epochs; a QC report flags any issues.
7. **Variable calculation** — Epoch counts, percentages, transitions, and bouts are computed at daily, average, weekday/weekend, and work/normal aggregation levels.

## Output Variables

See [variables.md](variables.md) for full documentation of every output column, including formulas and units.

## Project Structure

```
axivity-pp-v2/
    config.yaml          Configuration file
    main.py              Full analysis pipeline
    main_ot.py           Work-shift only pipeline
    variables.md         Output variable documentation
    data/                Input CSV files (default location)
    results/             Output files
    utils/
        activity.py      Activity epoch counting (total, work, normal splits)
        bout.py          Bout detection with noise tolerance
        transition.py    Active-inactive transition (AIT) counting
        df_filter.py     End trimming, activity recoding, day quality filters
        other_time.py    Work-shift time parsing and epoch mapping
        calc_var.py      Averages, weekday/weekend, daily, and OT summaries
        barcode.py       Barcode-style activity visualisation plots
```
