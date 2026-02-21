"""Activity intensity transition (AIT) counting."""

from itertools import groupby
from operator import itemgetter


def calculate_transitions(df, start, end, ai_column):
    """Count the number of distinct active bouts (I->A transitions) in a range."""
    active_indices = [i for i, val in enumerate(df[ai_column][start:end]) if val == 'A']
    active_groups = [list(map(itemgetter(1), g))[0] for k, g in
                     groupby(enumerate(active_indices), lambda ix: ix[0] - ix[1])]
    return len(active_groups)


def get_ait(df, index, date_info, ot_index, ot_date_info, run_ot, ai_column):
    """Calculate AIT per day, with optional work/normal split."""
    ait = {}

    for day, value in index.items():
        ait[day] = {'total': calculate_transitions(df, value[0], value[1], ai_column)}
        if run_ot and ot_index:
            ait[day]['ot'] = get_wrk_ait(df, index, date_info, ot_index, ot_date_info, day, ai_column)
            ait[day]['normal'] = ait[day]['total'] - ait[day]['ot']
    return ait


def get_wrk_ait(df, index, date_info, ot_index, ot_date_info, day, ai_column):
    """Sum AIT counts for all work shifts overlapping a given day."""
    from utils.activity import find_keys_for_date
    current_keys = find_keys_for_date(ot_date_info, date_info[day]['date'])
    prev_keys = find_keys_for_date(ot_date_info, date_info[day - 1]['date']) if day != 1 else []

    temp = 0
    for key in current_keys:
        temp += calculate_transitions(df, ot_index[key][0], min(ot_index[key][1], index[day][1]), ai_column)
    for key in prev_keys:
        if ot_index[key][1] > index[day][0]:
            temp += calculate_transitions(df, index[day][0], min(ot_index[key][1], index[day][1]), ai_column)

    return temp
