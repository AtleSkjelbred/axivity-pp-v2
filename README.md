# Axivity Post-Processing Pipeline v2

Post-processing pipeline for epoch-level accelerometer data exported from Axivity sensors. For each subject it filters the data, segments it into days, optionally maps work shifts, and computes a wide range of activity variables.

Configuration is read from `config.yaml` in the working directory. Results are written to `./results/` as separate CSV files, each controlled by config toggles.

## Getting Started

### Requirements

- Python 3.9+
- [Anaconda](https://www.anaconda.com/) (recommended) or any Python environment manager

### First-time setup

Open a terminal (Anaconda Prompt on Windows):

1. Clone the repository:
   ```
   git clone https://github.com/AtleSkjelbred/axivity-pp-v2.git
   ```
2. Navigate to the project folder:
   ```
   cd axivity-pp-v2
   ```
3. Create and activate a Python environment:
   ```
   conda create --name axivity python=3.9 --no-default-packages -y
   conda activate axivity
   ```
4. Install dependencies:
   ```
   pip install pandas pyyaml matplotlib numpy
   ```

### Updating an existing installation

If you already have the codebase and environment set up:

1. Navigate to the project folder:
   ```
   cd axivity-pp-v2
   ```
2. Check if you are up to date:
   ```
   git status
   ```
3. Pull the latest changes:
   ```
   git pull
   ```
4. Activate the environment:
   ```
   conda activate axivity
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

If work-shift analysis is enabled (`ot_variables: True`), a separate CSV file with shift start/end times is required. Set the path in `config.yaml` under `ot_path`. The file format is auto-detected (date-time columns or numeric indices).

## Usage

Activate the environment first:
```
conda activate axivity
```

```
python main.py [--data-folder PATH]
```

If `--data-folder` is omitted, defaults to `./data/`.

### Output

Each run creates a timestamped subfolder under `./results/` containing the output files:

| File | Config toggle | Description |
|------|--------------|-------------|
| `post process data.csv` | `base_variables` | Subject metadata, epoch info, recording dates, non-wear totals |
| `average data.csv` | `average_variables` | Per-day averages (includes weekday/weekend if `week_wknd_variables` is True) |
| `daily data.csv` | `daily_variables` | Per-day breakdown of all enabled variables |
| `ot data.csv` | `ot_variables` | Per-shift variables for all raw work shifts (no filtering) |
| `between ot data.csv` | `between_ot_variables` | Modified shifts (after min_shift filtering and merging) and between-ot period variables |
| `other time qc.csv` | `ot_variables` | QC report for work-shift matching |
| `config.yaml` | `save_config` | Snapshot of the config used for the run |
| `error_log.csv` | When errors occur | Files that failed processing |
| `barcode/` | `barcode_run` | Activity barcode plots |

## Configuration

All settings are in `config.yaml`. The file is commented — see it for full details. Key sections are summarised below.

### Column names

Map the column names in your input CSVs to what the pipeline expects. Change these if your files use different headers.

### End trimming

Scans inward from both ends of a recording and trims epochs that are dominated by non-wear or a single posture (likely sensor malfunction or the sensor lying on a table). Controlled by:

- `nw_ends` / `bug_ends` — enable trimming for non-wear / buggy postures
- `*_ends_min` — window size in minutes
- `*_ends_pct` — fraction threshold (trim if the target code exceeds this fraction of the window)

### Day quality filters

Entire days are removed if they are dominated by non-wear or a single posture:

- `nw_days` / `bug_days` — enable day-level filtering
- `nw_days_pct` / `bug_days_pct` — fraction thresholds
- `bug_lying`, `bug_sitting`, `bug_standing` — which postures to check (requires `bug_days`)
- `min_days` — minimum valid days required to produce output for a subject
- `remove_partial_days` — optionally remove days shorter than 24 hours

### Activity code remapping

Before analysis, activity codes can be simplified. Built-in remappings are applied first, followed by any custom `code_remap` entries:

| Setting | Effect |
|---------|--------|
| `remove_stairs` | Remap stair ascend/descend to walking |
| `remove_bending` | Remap bending to standing |
| `remove_shuffling` | Remap shuffling to walking (if between walks) or standing |
| `merge_cyc_codes` | Merge all cycling variants into a single cycling code |
| `adjust_cyc_interval` | Replace short cycling bouts (<=`min_cyc_epochs`) with surrounding activity |
| `code_remap` | Custom code-to-code remapping using names from the `codes` block (applied after built-in remappings) |

### Variable calculation

Toggle which categories of variables are calculated:

| Setting | Variables |
|---------|-----------|
| `act_variables` | Activity counts and percentages per activity code |
| `ai_variables` | Active/inactive time counts and percentages |
| `walk_variables` | Walking intensity counts and percentages |
| `ait_variables` | Active-inactive transition counts |
| `bout_variables` | Bout counts per duration category |
| `nw_variables` | Non-wear percentages |

### Output files

Toggle which CSV files are written to results/:

| Setting | Output | Requires |
|---------|--------|----------|
| `base_variables` | Subject metadata and recording info | — |
| `daily_variables` | Per-day breakdown | — |
| `average_variables` | Averages across all valid days | — |
| `week_wknd_variables` | Weekday/weekend averages (in average file) | `average_variables` |
| `ot_variables` | Raw work-shift breakdown + QC | valid `ot_path` |
| `between_ot_variables` | Modified shifts and between-ot breakdown (separate file) | `ot_variables` |
| `save_config` | Config snapshot | — |

Output formatting:

| Setting | Default | Description |
|---------|---------|-------------|
| `long_format` | `False` | Output daily and OT data in long format (one row per day/shift) instead of wide format (one row per subject) |
| `na_rep` | `''` | Representation for missing data in output CSVs (e.g. `''`, `'.'`, `'NA'`) |

### Work-shift settings

| Setting | Description |
|---------|-------------|
| `ot_path` | Path to the work-times CSV file |
| `ot_delimiter` | Delimiter of the work-times CSV (e.g. `;`) |
| `min_shift_minutes` | Shifts shorter than this are excluded as standalone shifts and do not interrupt between-ot sections. Default: 60 |

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

Code lists such as `act_codes`, `stair_codes`, and `cyc_codes` are derived automatically from the `codes` block at runtime. `bout_codes` references code names from the `codes` block and is resolved at runtime.

## Processing Steps

1. **Epoch detection** — The epoch rate is auto-detected from the first two timestamps.
2. **End trimming** — Non-wear and buggy posture epochs are trimmed from recording start/end.
3. **Activity recoding** — Built-in remappings (stairs, bending, shuffling, cycling) are applied, followed by custom `code_remap` entries. An active/inactive (AI) column is derived (sitting and lying = Inactive, everything else = Active).
4. **Day segmentation** — The recording is split into days at midnight boundaries.
5. **Day filtering** — Days failing quality checks are removed; remaining days are renumbered 1..N.
6. **Work-shift mapping** — If `ot_variables` is enabled, shift times are matched to accelerometer epochs; a QC report flags any issues.
7. **Variable calculation** — Epoch counts, percentages, transitions, and bouts are computed at daily, average, weekday/weekend, and work/normal aggregation levels. Results are written to separate CSV files based on config toggles.

## Output Variables

See [variables.md](variables.md) for full documentation of every output column, including formulas and units.

## Project Structure

```
axivity-pp-v2/
    config.yaml          Configuration file
    main.py              Analysis pipeline
    variables.md         Output variable documentation
    data/                Input CSV files (default location)
    results/             Output files
    utils/
        activity.py      Activity epoch counting (total, work, normal splits)
        bout.py          Bout detection with noise tolerance
        transition.py    Active-inactive transition (AIT) counting
        df_filter.py     End trimming, activity recoding, day quality filters
        other_time.py    Work-shift time parsing, epoch mapping, between-ot logic
        calc_var.py      Averages, weekday/weekend, daily, OT, and between-OT summaries
        barcode.py       Barcode-style activity visualisation plots
```
