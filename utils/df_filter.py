"""Data filtering: non-wear end trimming, activity code remapping, and day quality checks."""

import pandas as pd
from itertools import groupby
from operator import itemgetter


def filter_dataframe(new_line, df, epm, settings):
    """Apply end-trimming and activity code remapping to the raw dataframe."""
    if not settings['nw_ends'] and not settings['bug_ends']:
        df = filter_predictions(df, settings)
        return df
    df = non_wear_ends(new_line, df, epm, settings)

    df = filter_predictions(df, settings)

    return df


def non_wear_ends(new_line, df, epm, settings) -> pd.DataFrame:
    """Trim non-wear and buggy posture epochs from the start and end of the recording."""
    codes = settings['codes']
    start_len = len(df)
    if settings['nw_column'] in df.columns and settings['nw_ends']:
        df = remove_inactive_ends(df, settings['nw_column'], codes['nw_no_sensors'], epm * settings['nw_ends_min'], settings['nw_ends_pct'])
    if settings['bug_ends']:
        df = remove_inactive_ends(df, settings['act_column'], codes['lying'], epm * settings['bug_ends_min'], settings['bug_ends_pct'])
        df = remove_inactive_ends(df, settings['act_column'], codes['sitting'], epm * settings['bug_ends_min'], settings['bug_ends_pct'])
    end_len = len(df)

    new_line['epochs_removed'] = (start_len - end_len)

    return df


def remove_inactive_ends(df, column, code, length, threshold) -> pd.DataFrame:
    """Remove contiguous blocks of a given code from both ends of the dataframe.

    Scans inward from each end; stops when a window of `length` epochs
    has less than `threshold` fraction of the target code.
    """
    for i in reversed(range(len(df))):
        if df[column][i] != code:
            start_idx = max(0, i - length)
            window = i - start_idx
            if window > 0 and (df[column][start_idx: i] == code).sum() < window * threshold:
                df = df.iloc[:i + 1]
                break

    for i in range(len(df)):
        if df[column][i] != code:
            end_idx = min(len(df), i + length)
            window = end_idx - i
            if window > 0 and (df[column][i:end_idx] == code).sum() < window * threshold:
                df = df.iloc[i:]
                break
    df.reset_index(drop=True, inplace=True)
    return df


def filter_predictions(df: pd.DataFrame, settings: dict) -> pd.DataFrame:
    """Remap activity codes (stairs, bending, shuffling, cycling) and create the AI column."""
    codes = settings['codes']
    act_col = settings['act_column']

    if settings['remove_stairs']:
        stair_codes = [codes['stairs_ascend'], codes['stairs_descend']]
        for i in df.index[df[act_col].isin(stair_codes)].tolist():
            df.at[i, act_col] = codes['walking']
            if settings['walk_column'] in df.columns:
                df.at[i, settings['walk_column']] = settings['stair_walk_code']

    if settings['remove_bending']:
        for i in df.index[df[act_col] == codes['bending']].tolist():
            df.at[i, act_col] = codes['standing']

    if settings['remove_shuffling']:
        for i in df.index[df[act_col] == codes['shuffling']].tolist():
            try:
                if i > 0 and df[act_col][i - 1] == codes['walking'] and df[act_col][i + 1] == codes['walking']:
                    df.at[i, act_col] = codes['walking']
                else:
                    df.at[i, act_col] = codes['standing']
            except KeyError:
                df.at[i, act_col] = codes['standing']

    if settings['merge_cyc_codes']:
        cyc_all = [codes['cycling'], codes['cycling_stand'], codes['cyc_sit_inactive'], codes['cyc_stand_inactive']]
        df[act_col] = df[act_col].replace(cyc_all, codes['cycling'])

    if settings['adjust_cyc_interval']:
        df = cycling_interval(df, settings)

    if settings['ai_variables'] or settings['ait_variables']:
        inactive = [codes['sitting'], codes['lying']]
        df[settings['ai_column']] = ['I' if i in inactive else 'A' for i in df[act_col]]
    return df


def cycling_interval(df, settings) -> pd.DataFrame:
    """Replace short cycling bouts (<=min_cyc_epochs) with the surrounding activity code."""
    cycling_code = settings['codes']['cycling']
    temp = [ind for ind, item in enumerate(df[settings['act_column']]) if item == cycling_code]
    lens = [len(list(map(itemgetter(1), g))) for k, g in groupby(enumerate(temp), lambda ix: ix[0] - ix[1])]
    starts = [(list(map(itemgetter(1), g))[0]) for k, g in groupby(enumerate(temp), lambda ix: ix[0] - ix[1])]

    for start, length in zip(starts, lens):
        if length <= settings['min_cyc_epochs']:
            try:
                df.loc[start:start + length - 1, settings['act_column']] = df[settings['act_column']][start - 1]
            except KeyError:
                df.loc[start:start + length - 1, settings['act_column']] = df[settings['act_column']][start + 1]
    return df


def filter_days(df, index, settings, epd):
    """Remove days that fail quality checks (excessive non-wear, single posture, partial)."""
    if not settings['nw_days'] and not settings['bug_days']:
        return df

    codes = settings['codes']
    conditions = []
    if settings['nw_days']:
        conditions.append((settings['nw_column'], codes['nw_no_sensors'], settings['nw_days_pct']))
    if settings['bug_days']:
        if settings['bug_lying']:
            conditions.append((settings['act_column'], codes['lying'], settings['bug_days_pct']))
        if settings['bug_sitting']:
            conditions.append((settings['act_column'], codes['sitting'], settings['bug_days_pct']))
        if settings['bug_standing']:
            conditions.append((settings['act_column'], codes['standing'], settings['bug_days_pct']))

    keys_to_delete = set()
    for day, (start, end) in index.items():
        if settings['remove_partial_days']:
            if index[day][1] - index[day][0] < epd:
                keys_to_delete.add(day)
        for con in conditions:
            if (df[con[0]][start:end].values == con[1]).sum() > ((end - start) * con[2]):
                keys_to_delete.add(day)
                break

    for day in keys_to_delete:
        del index[day]

    return df
