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
  5. Output: writes two CSVs to results/ — the main post-process data and a
     QC report for the work-shift matching.

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
    outgoing_qc = pd.DataFrame()
    outgoing_df = pd.DataFrame()
    error_log = []
    base_path = os.getcwd()
    results_path = os.path.join(base_path, 'results')

    if settings['ot_run']:
        ot_df = get_ot_df(settings['ot_path'], settings['ot_delimiter'])
    else:
        ot_df = False

    if settings['barcode_run']:
        barcode_path = os.path.join(base_path, 'barcode')
        if not os.path.exists(barcode_path):
            os.makedirs(barcode_path)

    for csvfile in glob.glob(os.path.join(data_folder, '*.csv')):
        df = pd.read_csv(csvfile)
        if settings['id_column'] not in df.columns:
            error_log.append({'file': csvfile, 'error': f"Missing id column '{settings['id_column']}'"})
            continue
        new_line = {'subject_id': df[settings['id_column']][0]}
        subject_id = new_line['subject_id']
        print(f'--- Processing file: {subject_id} ---')

        epm, epd = epoch_test(new_line, df, settings['time_column'])
        if epm is None:
            error_log.append({'file': csvfile, 'subject_id': subject_id, 'error': epd})
            continue

        df = filter_dataframe(new_line, df, epm, settings)

        index = get_index(df, settings['time_column'])
        filter_days(df, index, settings, epd)
        # Keys must be contiguous (1..N) before get_variables/get_date_info,
        # because get_wrk_act/get_wrk_bouts/get_wrk_ait use day - 1 lookups.
        index = shift_index_keys(index)
        ot_index, ot_qc = other_times(df, new_line['subject_id'], settings['ot_run'], ot_df, settings['time_column'])
        date_info = get_date_info(df, index, settings['time_column'])
        ot_date_info = get_date_info(df, ot_index, settings['time_column'])

        if index and len(index) >= settings['min_days']:
            variables = get_variables(new_line, epm, df, index, date_info, ot_index, ot_date_info, settings)
            calculate_variables(df, new_line, index, ot_index, date_info, ot_date_info, variables, epm, epd, settings)

        if ot_qc:
            outgoing_qc = pd.concat([pd.DataFrame(ot_qc, index=[0]), outgoing_qc], ignore_index=True)
        outgoing_df = pd.concat([pd.DataFrame(new_line, index=[0]), outgoing_df], ignore_index=True)

        if settings['barcode_run']:
            if index and len(index) >= settings['min_days']:
                plot, ot_plot = gen_plot(df, index, ot_index, epd, settings)
                plotter(plot, ot_plot, date_info, new_line['subject_id'], base_path, settings)

    if not os.path.exists(results_path):
        os.makedirs(results_path)

    timestamp = datetime.now().strftime("%d.%m.%Y %H.%M")
    outgoing_qc.to_csv(os.path.join(results_path, f'other time qc {timestamp}.csv'), index=False)
    outgoing_df.to_csv(os.path.join(results_path, f'post process data {timestamp}.csv'), index=False)

    if error_log:
        error_df = pd.DataFrame(error_log)
        error_df.to_csv(os.path.join(results_path, f'error_log {timestamp}.csv'), index=False)
        print(f'----- {len(error_log)} file(s) skipped due to errors. See error_log in results/ -----')

    end_time = time.time()
    print(f'----- Total run time: {end_time - start_time} sec -----')


def get_ot_df(ot_path, delim):
    """Read the work-times CSV into a DataFrame."""
    ot_df = pd.read_csv(ot_path, delimiter=delim, encoding='utf-8-sig')
    return ot_df


def get_variables(new_line, epm, df, index, date_info, ot_index, ot_date_info, settings) -> dict:
    """Collect per-day activity, AIT, and bout data for each enabled variable group."""
    variables = {}
    if settings['nw_variables']:
        variables['nw'] = non_wear_pct(new_line, df, index, settings)
    if settings['ai_variables']:
        variables['ai'] = get_activities(df, index, date_info, ot_index, ot_date_info,
                                         settings['ot_run'], settings['ai_codes'], settings['ai_column'])
    if settings['act_variables']:
        variables['act'] = get_activities(df, index, date_info, ot_index, ot_date_info,
                                          settings['ot_run'], settings['act_codes'],
                                          settings['act_column'])
    if settings['walk_variables']:
        variables['walk'] = get_activities(df, index, date_info, ot_index, ot_date_info,
                                           settings['ot_run'], settings['walk_codes'],
                                           settings['walk_column'])
    if settings['ait_variables']:
        variables['ait'] = get_ait(df, index, date_info, ot_index, ot_date_info, settings['ot_run'], settings['ai_column'])
    if settings['bout_variables']:
        variables['bout'] = get_bouts(df, index, date_info, ot_index, ot_date_info, epm, settings)
    return variables


def epoch_test(new_line: dict, df: pd.DataFrame, time_column: str) -> tuple[int, int]:
    """Detect epoch length from the first two timestamps.

    Returns (epm, epd) on success, or (None, error_message) on failure.
    Also writes epoch info into new_line.
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

    new_line.update({'epoch per min': epm,
                     'epoch per day': epd})
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


def non_wear_pct(new_line, df, ind, settings) -> dict:
    """Calculate non-wear percentage totals and per-day breakdowns."""
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

    for key, value in total.items():
        new_line[f'total_nw_code_{key}'] = value
    return daily


def manage_config(config):
    """Derive runtime code lists (act_codes, walk_codes, ai) from base config."""
    config['act_codes'] = list(config['act_codes_base'])
    if not config['merge_cyc_codes']:
        config['act_codes'].extend(config['cyc_codes'])
    if not config['remove_stairs']:
        config['act_codes'].extend(config['stair_codes'])

    config['walk_codes'] = list(config['walk_codes_base'])
    if config['remove_stairs']:
        config['walk_codes'].append(config['stair_walk_code'])

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
