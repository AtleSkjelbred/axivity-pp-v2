"""Standalone work-shift analysis pipeline.

Processes the same CSV files as main.py but focuses exclusively on
work-shift variables: for each subject it extracts shift periods and
the between-shift sections, then calculates epoch counts and activity
percentages for each section.

Short shifts (<=min_shift_minutes, default 60) are excluded as standalone
shifts and do not interrupt between-shift sections.

Usage:
    python main_ot.py [--data-folder PATH]

If --data-folder is omitted, defaults to ./data/. Configuration is read from
config.yaml in the working directory.
"""

import pandas as pd
import glob
import os
from datetime import datetime
import time
import argparse
import yaml

from utils.other_time import other_times
from utils.activity import count_codes
from utils.transition import calculate_transitions
from utils.bout import count_bouts
from utils.df_filter import filter_dataframe, filter_days
from main import manage_config, get_index, shift_index_keys


def main(data_folder, settings):
    """Batch-process all CSV files and compute per-shift activity variables."""
    results = []
    qc_results = []
    error_log = []
    base_path = os.getcwd()
    results_path = os.path.join(base_path, 'results')

    ot_df = get_ot_df(settings['ot_path'], settings['ot_delimiter'])

    for csvfile in glob.glob(os.path.join(data_folder, '*.csv')):
        df = pd.read_csv(csvfile)
        if settings['id_column'] not in df.columns:
            error_log.append({'file': csvfile, 'error': f"Missing id column '{settings['id_column']}'"})
            continue
        subject_id = df[settings['id_column']][0]
        print(f'--- Processing file: {subject_id} ---')

        epm, epd = epoch_test(df, settings['time_column'])
        if epm is None:
            error_log.append({'file': csvfile, 'subject_id': subject_id, 'error': epd})
            continue

        original_len = len(df)
        filter_info = {}
        df = filter_dataframe(filter_info, df, epm, settings)

        if settings['nw_days'] or settings['bug_days']:
            index = get_index(df, settings['time_column'])
            filter_days(df, index, settings, epd)
            if index:
                valid_rows = []
                for start, end in index.values():
                    valid_rows.extend(range(start, end))
                df = df.iloc[sorted(valid_rows)].reset_index(drop=True)

        ot_index, ot_qc = other_times(df, subject_id, True, ot_df, settings['time_column'])

        if ot_qc:
            qc_results.append(ot_qc)

        if ot_index:
            ot_index, between_index = get_between_shifts(ot_index, epm, epd, len(df), settings.get('min_shift_minutes', 60))
            new_line = build_output(df, subject_id, ot_index, between_index, epm, settings)
            new_line['epoch_per_min'] = epm
            new_line['epoch_per_day'] = epd
            new_line['total_epochs'] = original_len
            new_line['epochs_after_filter'] = len(df)
            new_line['epochs_removed'] = filter_info.get('epochs_removed', 0)
            results.append(new_line)

    if not os.path.exists(results_path):
        os.makedirs(results_path)

    timestamp = datetime.now().strftime("%d.%m.%Y %H.%M")
    outgoing_df = pd.DataFrame(results) if results else pd.DataFrame()
    outgoing_qc = pd.DataFrame(qc_results) if qc_results else pd.DataFrame()
    outgoing_df.to_csv(os.path.join(results_path, f'ot shift data {timestamp}.csv'), index=False)
    outgoing_qc.to_csv(os.path.join(results_path, f'ot shift qc {timestamp}.csv'), index=False)

    if error_log:
        error_df = pd.DataFrame(error_log)
        error_df.to_csv(os.path.join(results_path, f'ot error_log {timestamp}.csv'), index=False)
        print(f'----- {len(error_log)} file(s) skipped due to errors. See error_log in results/ -----')

    end_time = time.time()
    print(f'----- Total run time: {end_time - start_time} sec -----')


def get_ot_df(ot_path, delim):
    """Read the work-times CSV into a DataFrame."""
    return pd.read_csv(ot_path, delimiter=delim, encoding='utf-8-sig')


def get_between_shifts(ot_index, epm, epd, data_len, min_shift_minutes=60):
    """Build between-shift index ranges from the shift index.

    Shifts <=min_shift_minutes are excluded as standalone shifts. The
    between-shift section following each real shift extends to the start of
    the next real shift, capped at 24 hours and at data_len. Short shifts
    within between sections are carved out, resulting in multiple sub-ranges.

    Consecutive real shifts with no gap between them are merged into one.

    Returns (renumbered_shifts, between_dict) where between_dict maps
    each shift key to a list of [start, end) ranges.
    """
    min_shift = min_shift_minutes * epm
    real = {k: v for k, v in ot_index.items() if v[1] - v[0] > min_shift}
    short = sorted([v for k, v in ot_index.items() if v[1] - v[0] <= min_shift], key=lambda x: x[0])

    real_keys = sorted(real.keys())
    renumbered = {i + 1: real[k] for i, k in enumerate(real_keys)}

    # Merge consecutive shifts that are directly adjacent (no gap)
    renumbered = merge_adjacent_shifts(renumbered)

    between = {}
    for i, key in enumerate(sorted(renumbered.keys())):
        shift_end = renumbered[key][1]
        if i + 1 < len(renumbered):
            section_end = renumbered[sorted(renumbered.keys())[i + 1]][0]
        else:
            section_end = shift_end + epd
        section_end = min(section_end, shift_end + epd, data_len)

        ranges = []
        pos = shift_end
        for s_start, s_end in short:
            if s_start >= section_end:
                break
            if s_end <= pos:
                continue
            if s_start > pos:
                ranges.append([pos, s_start])
            pos = s_end
        if pos < section_end:
            ranges.append([pos, section_end])

        between[key] = ranges

    return renumbered, between


def merge_adjacent_shifts(shifts):
    """Merge consecutive shifts that are directly adjacent (next starts where previous ends)."""
    if len(shifts) <= 1:
        return shifts
    sorted_keys = sorted(shifts.keys())
    merged = {1: list(shifts[sorted_keys[0]])}
    current = 1
    for k in sorted_keys[1:]:
        if shifts[k][0] <= merged[current][1]:
            merged[current][1] = max(merged[current][1], shifts[k][1])
        else:
            current += 1
            merged[current] = list(shifts[k])
    return merged


def build_output(df, subject_id, ot_index, between_index, epm, settings):
    """Calculate epoch counts and activity percentages per shift and between section."""
    act_column = settings['act_column']
    ai_column = settings['ai_column']
    walk_column = settings['walk_column']
    act_codes = settings['act_codes']
    ai_codes = settings['ai_codes']
    walk_codes = settings['walk_codes']
    code_name = settings['code_name']
    nw_column = settings['nw_column']
    nw_codes = settings['nw_codes']
    line = {'subject_id': subject_id}

    bout_codes = settings['bout_codes']

    time_col = settings['time_column']

    for key in sorted(ot_index.keys()):
        # Shift variables
        start, end = ot_index[key]
        epochs = end - start
        prefix = f'shift{key}'
        line[f'{prefix}_start_datetime'] = datetime.strptime(df[time_col][start][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
        if end in df.index:
            line[f'{prefix}_end_datetime'] = datetime.strptime(df[time_col][end][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
        else:
            line[f'{prefix}_end_datetime'] = datetime.strptime(df[time_col][end - 1][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
        line[f'{prefix}_start_wkday_nr'] = datetime.strptime(df[time_col][start][:10], "%Y-%m-%d").weekday() + 1
        line[f'{prefix}_start_wkday_str'] = datetime.strptime(df[time_col][start][:10], "%Y-%m-%d").strftime('%A')
        line[f'{prefix}_epochs'] = epochs
        line[f'{prefix}_min'] = epochs / epm
        for code in ai_codes:
            count = count_codes(df, start, end, ai_column, code)
            line[f'{prefix}_{code_name[code]}_min'] = round(count / epm, 2)
            line[f'{prefix}_{code_name[code]}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None
        for code in act_codes:
            count = count_codes(df, start, end, act_column, code)
            line[f'{prefix}_{code_name[code]}_min'] = round(count / epm, 2)
            line[f'{prefix}_{code_name[code]}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None
        walk_total = sum(count_codes(df, start, end, walk_column, c) for c in walk_codes)
        for code in walk_codes:
            count = count_codes(df, start, end, walk_column, code)
            line[f'{prefix}_walk{code_name[code]}_min'] = round(count / epm, 2)
            line[f'{prefix}_walk{code_name[code]}_pct'] = round(count / walk_total * 100, 2) if walk_total > 0 else None
        for code in nw_codes:
            count = count_codes(df, start, end, nw_column, code)
            line[f'{prefix}_nw_code_{code}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None
        line[f'{prefix}_ait'] = calculate_transitions(df, start, end, ai_column)
        bouts = count_bouts(df, start, end, epm, settings)
        for code in bout_codes:
            for cat, val in enumerate(bouts[code]):
                line[f'{prefix}_{code_name[code]}_bout_c{cat + 1}'] = val

        # Between variables for the same key
        if key in between_index and between_index[key]:
            ranges = between_index[key]
            b_start = ranges[0][0]
            b_end = ranges[-1][1]
            epochs = sum(end - start for start, end in ranges)
            prefix = f'between{key}'
            line[f'{prefix}_start_datetime'] = datetime.strptime(df[time_col][b_start][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
            if b_end in df.index:
                line[f'{prefix}_end_datetime'] = datetime.strptime(df[time_col][b_end][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
            else:
                line[f'{prefix}_end_datetime'] = datetime.strptime(df[time_col][b_end - 1][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
            line[f'{prefix}_start_wkday_nr'] = datetime.strptime(df[time_col][b_start][:10], "%Y-%m-%d").weekday() + 1
            line[f'{prefix}_start_wkday_str'] = datetime.strptime(df[time_col][b_start][:10], "%Y-%m-%d").strftime('%A')
            line[f'{prefix}_epochs'] = epochs
            line[f'{prefix}_min'] = epochs / epm
            for code in ai_codes:
                count = sum(count_codes(df, start, end, ai_column, code) for start, end in ranges)
                line[f'{prefix}_{code_name[code]}_min'] = round(count / epm, 2)
                line[f'{prefix}_{code_name[code]}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None
            for code in act_codes:
                count = sum(count_codes(df, start, end, act_column, code) for start, end in ranges)
                line[f'{prefix}_{code_name[code]}_min'] = round(count / epm, 2)
                line[f'{prefix}_{code_name[code]}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None
            walk_total = sum(count_codes(df, start, end, walk_column, c) for start, end in ranges for c in walk_codes)
            for code in walk_codes:
                count = sum(count_codes(df, start, end, walk_column, code) for start, end in ranges)
                line[f'{prefix}_walk{code_name[code]}_min'] = round(count / epm, 2)
                line[f'{prefix}_walk{code_name[code]}_pct'] = round(count / walk_total * 100, 2) if walk_total > 0 else None
            for code in nw_codes:
                count = sum(count_codes(df, start, end, nw_column, code) for start, end in ranges)
                line[f'{prefix}_nw_code_{code}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None
            line[f'{prefix}_ait'] = sum(calculate_transitions(df, start, end, ai_column) for start, end in ranges)
            combined_bouts = {}
            for start, end in ranges:
                bouts = count_bouts(df, start, end, epm, settings)
                for code in bout_codes:
                    if code not in combined_bouts:
                        combined_bouts[code] = [0] * len(bouts[code])
                    combined_bouts[code] = [a + b for a, b in zip(combined_bouts[code], bouts[code])]
            for code in bout_codes:
                for cat, val in enumerate(combined_bouts.get(code, [])):
                    line[f'{prefix}_{code_name[code]}_bout_c{cat + 1}'] = val

    return line


def epoch_test(df, time_column):
    """Detect epoch length from the first two timestamps.

    Returns (epm, epd) on success, or (None, error_message) on failure.
    """
    if len(df) < 2:
        return None, f"Dataset too small for epoch detection ({len(df)} rows)"
    try:
        timestamps = pd.to_datetime(df[time_column].iloc[:2])
        epoch_seconds = (timestamps.iloc[1] - timestamps.iloc[0]).total_seconds()
    except Exception as e:
        return None, f"Failed to parse timestamps: {e}"
    if epoch_seconds <= 0:
        return None, f"Invalid epoch duration: {epoch_seconds} seconds"
    epm = int(60 / epoch_seconds)
    epd = epm * 60 * 24
    return epm, epd


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-folder', type=str, dest='data_folder', help='Path of dataset folder')
    args = parser.parse_args()
    if not args.data_folder:
        if not os.path.exists(os.path.join(os.getcwd(), 'data/')):
            os.makedirs(os.path.join(os.getcwd(), 'data/'))
        args.data_folder = os.path.join(os.getcwd(), 'data/')

    with open('config.yaml') as f:
        config = yaml.safe_load(f)
    manage_config(config)

    start_time = time.time()
    main(args.data_folder, config)
