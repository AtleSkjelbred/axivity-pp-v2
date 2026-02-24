"""Activity epoch counting: total, work-time, and normal-time breakdowns per day."""


def count_codes(df, start, end, column, code):
    """Count epochs matching `code` in df[column][start:end]."""
    return (df[column][start:end].values == code).sum()


def get_activities(df, index, date_info, ot_index, wrk_date_info, ot_variables, codes, column):
    """Count activity epochs per day, with optional work/normal split."""
    temp = {}

    for day, (start, end) in index.items():
        temp[day] = {}

        for code in codes:
            temp[day][code] = {'total': count_codes(df, start, end, column, code)}
            if ot_variables and ot_index:
                temp[day][code]['ot'] = get_wrk_act(df, index, date_info, ot_index, wrk_date_info, day,
                                                    column, code)
                temp[day][code]['normal'] = temp[day][code]['total'] - temp[day][code]['ot']

    return temp


def get_wrk_act(df, index, date_info, wrk_index, wrk_date_info, day, column, code):
    """Sum activity counts for all work shifts overlapping a given day."""
    current_keys = find_keys_for_date(wrk_date_info, date_info[day]['date'])
    prev_keys = find_keys_for_date(wrk_date_info, date_info[day - 1]['date']) if day != 1 else []

    temp = 0
    for key in current_keys:
        temp += count_codes(df, wrk_index[key][0], min(wrk_index[key][1], index[day][1]), column, code)
    for key in prev_keys:
        if wrk_index[key][1] > index[day][0]:
            temp += count_codes(df, index[day][0], min(wrk_index[key][1], index[day][1]), column, code)
    return temp


def find_keys_for_date(date_info, target_date):
    """Return all index keys whose date matches target_date."""
    return [key for key, info in date_info.items() if info['date'] == target_date]
