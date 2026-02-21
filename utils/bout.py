"""Activity bout detection: identifies continuous bouts with noise tolerance,
categorized by duration thresholds.
"""


def get_bouts(df, index, date_info, wrk_index, wrk_date_info, epm, settings):
    """Count bout categories per day, with optional work/normal split."""
    temp = {}
    for day, (start, end) in index.items():
        temp[day] = {}
        temp[day]['total'] = count_bouts(df, start, end, epm, settings)
        if settings['ot_run'] and wrk_index:
            temp[day]['other'] = get_wrk_bouts(df, index, date_info, wrk_index, wrk_date_info, day, epm, settings)
            temp[day]['normal'] = {key: [a - b for a, b in zip(temp[day]['total'][key], temp[day]['other'][key])]
                                   for key in temp[day]['total']}

    return temp


def get_wrk_bouts(df, index, date_info, wrk_index, wrk_date_info, day, epm, settings):
    """Sum bout counts for all work shifts overlapping a given day."""
    from utils.activity import find_keys_for_date
    current_keys = find_keys_for_date(wrk_date_info, date_info[day]['date'])
    prev_keys = find_keys_for_date(wrk_date_info, date_info[day - 1]['date']) if day != 1 else []

    codes = settings['bout_codes']
    i_cat = settings['i_cat']
    empty = {key: [0 for _ in range(len(i_cat))] for key in codes}

    result = {key: list(empty[key]) for key in codes}
    for k in current_keys:
        bouts = count_bouts(df, wrk_index[k][0], min(wrk_index[k][1], index[day][1]), epm, settings)
        result = {key: [a + b for a, b in zip(result[key], bouts[key])] for key in codes}
    for k in prev_keys:
        if wrk_index[k][1] > index[day][0]:
            bouts = count_bouts(df, index[day][0], min(wrk_index[k][1], index[day][1]), epm, settings)
            result = {key: [a + b for a, b in zip(result[key], bouts[key])] for key in codes}

    return result


def count_bouts(df, start, end, epm, settings):
    """Detect and categorize activity bouts in a range, with noise tolerance."""
    column = settings['act_column']
    codes = settings['bout_codes']
    max_noise = settings['noise_threshold']
    cut = settings['length_threshold']

    temp = {key: [] for key in codes}
    selected_values = df[column].iloc[start:end]

    if df[column][start] in codes:
        current_code = df[column][start]
        jump = 0
    else:
        jump = skip(df, start, codes, column, end)
        current_code = df[column][start + jump]
    length, noise = 0, 0

    for i, value in selected_values.items():
        if jump > 0:
            jump -= 1
            continue

        if value != current_code:
            epoch_gap = find_next(df, current_code, i, column, end)
            if length > 0 and (((length / epm) < cut and epoch_gap < 2) or
            ((length / epm) > cut and epoch_gap < 3 and noise / length < max_noise)):
                noise, length = noise + 1, length + 1
            else:
                prev_matches = i > 0 and df[column][i - 1] == current_code
                temp[current_code].append(length if prev_matches else length - 1)
                length = 1 if prev_matches else 2
                noise = 0
                try:
                    current_code = value if value in codes else df[column][i + skip(df, i, codes, column, end)]
                except KeyError:
                    break
        else:
            length += 1
    temp[current_code].append(length)
    return get_bout_categories(temp, epm, settings)


def skip(df, index, codes, column, end) -> int:
    """Count epochs until the next tracked activity code is found."""
    count = 1
    try:
        while index + count < end and df[column][index + count] not in codes:
            count += 1
        return count
    except KeyError:
        return count


def find_next(df, code, index, column, end) -> int:
    """Count epochs until the next occurrence of `code` is found."""
    count = 1
    try:
        while index + count < end and df[column][index + count] != code:
            count += 1
        return count
    except KeyError:
        return count


def get_bout_categories(bout_dict, epm, settings):
    """Bin detected bout lengths into duration categories (e.g. short, medium, long)."""
    i_cat = settings['i_cat']
    a_cat = settings['a_cat']
    codes = settings['codes']
    inactive_codes = [codes['sitting'], codes['lying']]
    epochs_per_sec = epm / 60

    bouts = {}
    for code, bout_lengths in bout_dict.items():
        cat = i_cat if code in inactive_codes else a_cat
        bouts[code] = []
        for cat_key in cat:
            lower = cat[cat_key][0] * epochs_per_sec
            upper = cat[cat_key][1] * epochs_per_sec
            count = sum(1 for bout in bout_lengths if lower <= bout <= upper)
            bouts[code].append(count)
    return bouts
