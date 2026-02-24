"""Main pipeline for post-processing accelerometer data.

Processes CSV files exported from Axivity sensors through a series of steps:
  1. Data filtering: trims non-wear ends, remaps activity codes (stairs, bending,
     shuffling, cycling), and creates the active/inactive (AI) column.
  2. Day segmentation: splits the recording into days at midnight boundaries,
     removes days that fail quality checks (excessive non-wear, single-posture).
  3. Work shift extraction: parses shift times from a separate work-times file,
     maps them onto the data, and splits each day's activity into 'normal' vs
     'other' (work) time.
  4. Variable calculation: computes per-day, per-shift, average, and
     weekday/weekend summary statistics for activity counts, active/inactive
     time, activity intensity transitions (AIT), and bout counts.
  5. Output: writes separate CSVs to results/, each controlled by config toggles:
     - post process data: always (base subject metadata)
     - average data: when average_variables is True (includes weekday/weekend)
     - daily data: when daily_variables is True
     - ot data: when ot_variables is True (shifts + between-ot sections)
     - other time qc: when ot_variables is True

Usage:
    python main.py [--data-folder PATH]

If --data-folder is omitted, defaults to ./data/. Configuration is read from
config.yaml in the working directory.
"""

import pandas as pd
import glob
import os
from datetime import datetime
import time
from itertools import groupby
from operator import itemgetter
import argparse
import yaml

from utils.df_filter import filter_dataframe, filter_days
from utils.activity import get_activities
from utils.transition import get_ait
from utils.bout import get_bouts
from utils.calc_var import calculate_variables
from utils.other_time import other_times
from utils.barcode import gen_plot, plotter


def main(data_folder, settings):
    """Batch-process all CSV files in data_folder and write results."""
    base_results = []
    avg_results = []
    daily_results = []
    ot_results = []
    qc_results = []
    error_log = []
    base_path = os.getcwd()
    timestamp = datetime.now().strftime("%d.%m.%Y %H.%M")
    run_path = os.path.join(base_path, 'results', timestamp)
    os.makedirs(run_path, exist_ok=True)

    if settings['ot_variables']:
        ot_df = get_ot_df(settings['ot_path'], settings['ot_delimiter'])
    else:
        ot_df = False

    if settings['barcode_run']:
        barcode_path = os.path.join(run_path, 'barcode')
        os.makedirs(barcode_path, exist_ok=True)

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

        total_epochs = len(df)
        timestamps = pd.to_datetime(df[settings['time_column']])
        recording_start = timestamps.iloc[0].strftime("%d.%m.%Y %H:%M")
        recording_end = timestamps.iloc[-1].strftime("%d.%m.%Y %H:%M")

        filter_info = {}
        df = filter_dataframe(filter_info, df, epm, settings)

        index = get_index(df, settings['time_column'])
        total_days = len(index)
        filter_days(df, index, settings, epd)
        days_removed = total_days - len(index)
        # Keys must be contiguous (1..N) before get_variables/get_date_info,
        # because get_wrk_act/get_wrk_bouts/get_wrk_ait use day - 1 lookups.
        index = shift_index_keys(index)
        ot_index, ot_qc = other_times(df, subject_id, settings['ot_variables'], ot_df, settings['time_column'])
        date_info = get_date_info(df, index, settings['time_column'])
        ot_date_info = get_date_info(df, ot_index, settings['time_column'])

        base_line = {'subject_id': subject_id, 'epoch_per_min': epm, 'epoch_per_day': epd,
                     'total_epochs': total_epochs,
                     'recording_start': recording_start, 'recording_end': recording_end,
                     'epochs_removed': filter_info.get('epochs_removed', 0),
                     'total_days_before_filter': total_days, 'days_removed': days_removed}

        if index and len(index) >= settings['min_days']:
            variables, nw_total = get_variables(epm, df, index, date_info, ot_index, ot_date_info, settings)
            for code, value in nw_total.items():
                base_line[f'total_nw_code_{code}'] = value
            var_results = calculate_variables(df, subject_id, index, ot_index, date_info,
                                             ot_date_info, variables, epm, epd, settings)
            base_line.update(var_results['base'])
            if 'average' in var_results:
                avg_results.append(var_results['average'])
            if 'daily' in var_results:
                if settings['long_format']:
                    daily_results.extend(var_results['daily'])
                else:
                    daily_results.append(var_results['daily'])
            if 'ot' in var_results:
                if settings['long_format']:
                    ot_results.extend(var_results['ot'])
                else:
                    ot_results.append(var_results['ot'])

        if ot_qc:
            qc_results.append(ot_qc)
        base_results.append(base_line)

        if settings['barcode_run']:
            if index and len(index) >= settings['min_days']:
                plot, ot_plot = gen_plot(df, index, ot_index, epd, settings)
                plotter(plot, ot_plot, date_info, subject_id, run_path, settings)

    na_rep = settings.get('na_rep', '')

    if settings['base_variables'] and base_results:
        pd.DataFrame(base_results).to_csv(
            os.path.join(run_path, 'post process data.csv'), index=False, na_rep=na_rep)

    if settings['average_variables'] and avg_results:
        pd.DataFrame(avg_results).to_csv(
            os.path.join(run_path, 'average data.csv'), index=False, na_rep=na_rep)

    if settings['daily_variables'] and daily_results:
        pd.DataFrame(daily_results).to_csv(
            os.path.join(run_path, 'daily data.csv'), index=False, na_rep=na_rep)

    if settings['ot_variables'] and ot_results:
        pd.DataFrame(ot_results).to_csv(
            os.path.join(run_path, 'ot data.csv'), index=False, na_rep=na_rep)

    if qc_results:
        pd.DataFrame(qc_results).to_csv(
            os.path.join(run_path, 'other time qc.csv'), index=False, na_rep=na_rep)

    if settings.get('save_config', False):
        with open(os.path.join(run_path, 'config.yaml'), 'w') as f:
            yaml.dump(settings, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    if error_log:
        error_df = pd.DataFrame(error_log)
        error_df.to_csv(os.path.join(run_path, 'error_log.csv'), index=False, na_rep=na_rep)
        print(f'----- {len(error_log)} file(s) skipped due to errors. See error_log in results/ -----')

    end_time = time.time()
    print(f'----- Total run time: {end_time - start_time} sec -----')


def get_ot_df(ot_path, delim):
    """Read the work-times CSV into a DataFrame."""
    ot_df = pd.read_csv(ot_path, delimiter=delim, encoding='utf-8-sig')
    return ot_df


def get_variables(epm, df, index, date_info, ot_index, ot_date_info, settings) -> tuple[dict, dict]:
    """Collect per-day activity, AIT, and bout data for each enabled variable group.

    Returns (variables, nw_total) where nw_total maps codes to overall nw percentages.
    """
    variables = {}
    nw_total = {}
    if settings['nw_variables']:
        nw_total, nw_daily = non_wear_pct(df, index, settings)
        variables['nw'] = nw_daily
    if settings['ai_variables']:
        variables['ai'] = get_activities(df, index, date_info, ot_index, ot_date_info,
                                         settings['ot_variables'], settings['ai_codes'], settings['ai_column'])
    if settings['act_variables']:
        variables['act'] = get_activities(df, index, date_info, ot_index, ot_date_info,
                                          settings['ot_variables'], settings['act_codes'],
                                          settings['act_column'])
    if settings['walk_variables']:
        variables['walk'] = get_activities(df, index, date_info, ot_index, ot_date_info,
                                           settings['ot_variables'], settings['walk_codes'],
                                           settings['walk_column'])
    if settings['ait_variables']:
        variables['ait'] = get_ait(df, index, date_info, ot_index, ot_date_info, settings['ot_variables'], settings['ai_column'])
    if settings['bout_variables']:
        variables['bout'] = get_bouts(df, index, date_info, ot_index, ot_date_info, epm, settings)
    return variables, nw_total


def epoch_test(df: pd.DataFrame, time_column: str) -> tuple[int, int]:
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


def get_index(df: pd.DataFrame, time_column: str) -> dict:
    """Split the recording into days at midnight boundaries.

    Returns a dict mapping day number to [start_idx, end_idx) ranges.
    """
    timestamps = pd.to_datetime(df[time_column])
    midnight_mask = (timestamps.dt.hour == 0) & (timestamps.dt.minute == 0)
    index = [(list(map(itemgetter(1), g))[0]) for k, g in
             groupby(enumerate(df.index[midnight_mask].tolist()), lambda ix: ix[0] - ix[1])]

    if 0 not in index:
        index.insert(0, 0)
    if len(df) not in index:
        index.append(len(df))

    index_dict = {i + 1: [index[i], index[i + 1]] for i in range(len(index) - 1)}
    return index_dict


def shift_index_keys(index: dict) -> dict:
    """Renumber index keys to contiguous 1..N after day filtering."""
    sorted_keys = sorted(index.keys())
    shifted_dict = {new_key: index[old_key] for new_key, old_key in enumerate(sorted_keys, start=1)}
    return shifted_dict


def get_date_info(df, index, time_column):
    """Build a dict of weekday number, weekday name, date, and epoch length per day."""
    if not index:
        return False
    info = {day: {
        'day_nr': datetime.strptime(df[time_column][val[0]][:10], "%Y-%m-%d").weekday() + 1,
        'day_str': datetime.strptime(df[time_column][val[0]][:10], "%Y-%m-%d").strftime('%A'),
        'date': df[time_column][val[0]][:10], 'length_epoch': index[day][1] - index[day][0]}
        for day, val in index.items()}
    return info


def non_wear_pct(df, ind, settings) -> tuple[dict, dict]:
    """Calculate non-wear percentage totals and per-day breakdowns.

    Returns (total_dict, daily_dict) where total_dict maps codes to overall
    percentages and daily_dict maps days to per-code percentages.
    """
    temp = {day: {code: (df[settings['nw_column']][start: end].values == code).sum() for code in settings['nw_codes']}
            for day, (start, end) in ind.items()}

    total_epochs = sum(e - s for s, e in ind.values())
    total = {code: round(sum([temp[day][code] for day in ind.keys()]) / total_epochs * 100, 2)
             if total_epochs > 0 else 0.0 for code in settings['nw_codes']}

    daily = {}
    for day in ind.keys():
        day_length = ind[day][1] - ind[day][0]
        daily[day] = {code: round(temp[day][code] / day_length * 100, 2)
                      if day_length > 0 else 0.0 for code in settings['nw_codes']}

    return total, daily


def manage_config(config):
    """Derive runtime code lists (act_codes, walk_codes, ai) from base config."""
    codes = config['codes']

    config['stair_codes'] = [codes['stairs_ascend'], codes['stairs_descend']]
    config['cyc_codes'] = [codes['cycling_stand'], codes['cyc_sit_inactive'], codes['cyc_stand_inactive']]

    config['act_codes'] = [codes['walking'], codes['running'], codes['standing'],
                           codes['sitting'], codes['lying'], codes['cycling']]
    if not config['merge_cyc_codes']:
        config['act_codes'].extend(config['cyc_codes'])
    if not config['remove_stairs']:
        config['act_codes'].extend(config['stair_codes'])

    config['bout_codes'] = [codes[name] for name in config['bout_codes']]

    config['walk_codes'] = list(config['walk_codes_base'])
    if config['remove_stairs']:
        config['walk_codes'].append(config['stair_walk_code'])

    config['code_remap'] = {codes[src]: codes[tgt] for src, tgt in config['code_remap'].items()}

    config['ai_column'] = 'ai_column'
    config['ai_codes'] = ['A', 'I']

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
