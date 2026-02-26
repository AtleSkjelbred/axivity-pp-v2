"""Variable calculation: averages, weekday/weekend, daily, and work-shift summaries."""

from utils.transition import calculate_transitions
from utils.activity import count_codes
from utils.bout import count_bouts
from utils.other_time import get_between_ot
from datetime import datetime


def calculate_variables(df, subject_id, index, ot_index, date_info, ot_date_info, variables, epm, epd, settings):
    """Compute all enabled summary statistics.

    Returns a dict of separate result dicts for each output group:
        {'base': dict, 'average': dict, 'daily': dict, 'ot': dict}
    Only enabled groups are included.
    """
    temp = {'ai': ['ai_codes', 'ai_column'], 'act': ['act_codes', 'act_column'], 'walk': ['walk_codes', 'walk_column']}
    chosen_var = {key: {'codes': settings[codes], 'column': settings[column]}
                  for key, (codes, column) in temp.items() if key in variables}
    code_name = settings['code_name']
    bout_codes = settings['bout_codes']
    results = {}

    base_line = {'subject_id': subject_id}
    wk_wknd = weekday_distribution(base_line, index, date_info, epm)
    if ot_index:
        base_line['nr_ot'] = len(ot_index)
    results['base'] = base_line

    if settings['average_variables']:
        avg_line = {'subject_id': subject_id,
                    'total_days': round(wk_wknd['total'], 2),
                    'wk_days': round(wk_wknd['wk'], 2),
                    'wknd_days': round(wk_wknd['wknd'], 2)}
        average_variables(avg_line, variables, index, wk_wknd, epm, epd, code_name, chosen_var, bout_codes)
        if settings['week_wknd_variables']:
            wk_wknd_variables(avg_line, variables, index, date_info, wk_wknd, epm, epd, code_name, chosen_var, bout_codes)
        results['average'] = avg_line

    if settings['daily_variables']:
        if settings['long_format']:
            results['daily'] = daily_variables_long(subject_id, variables, date_info, code_name, epd)
        else:
            daily_line = {'subject_id': subject_id}
            daily_variables(daily_line, variables, date_info, code_name, epd)
            results['daily'] = daily_line

    if settings['ot_variables']:
        if settings['long_format']:
            ot_rows, between_rows = other_time_variables_long(subject_id, df, ot_index, ot_date_info,
                                                              code_name, chosen_var, bout_codes, settings, epm, epd)
            results['ot'] = ot_rows
            if between_rows:
                results['between_ot'] = between_rows
        else:
            ot_line = {'subject_id': subject_id}
            other_time_variables(ot_line, df, ot_index, ot_date_info, code_name, chosen_var, bout_codes, settings, epm)
            results['ot'] = ot_line
            if settings['between_ot_variables']:
                between_line = {'subject_id': subject_id}
                between_time_variables(between_line, df, ot_index, code_name, chosen_var, bout_codes, settings, epm, epd)
                results['between_ot'] = between_line

    return results


def weekday_distribution(new_line, index, date_info, epm) -> dict:
    """Count total, weekday, and weekend day fractions."""
    wk_wknd = {'wk': [val[1] - val[0] for key, val in index.items() if date_info[key]['day_nr'] not in [6, 7]],
               'wknd': [val[1] - val[0] for key, val in index.items() if date_info[key]['day_nr'] in [6, 7]]}
    for key, val in wk_wknd.items():
        wk_wknd[key] = sum(val) / (epm * 60 * 24)
    wk_wknd['total'] = wk_wknd['wk'] + wk_wknd['wknd']
    new_line[f'total_days'] = round(wk_wknd['total'], 2)
    new_line[f'wk_days'] = round(wk_wknd['wk'], 2)
    new_line[f'wknd_days'] = round(wk_wknd['wknd'], 2)
    return wk_wknd


def average_variables(new_line, var, index, wk_wknd, epm, epd, code_name, chosen_var, bout_codes):
    """Calculate per-day averages for activity, AIT, and bout variables."""
    no_data = wk_wknd['total'] == 0

    for key, dic in chosen_var.items():
        if no_data:
            for code in dic['codes']:
                new_line[f'avg_{code_name[code]}_min'] = None
                new_line[f'avg_{code_name[code]}_pct'] = None
        else:
            temp = {code: [var[key][day][code]['total'] for day in index.keys()] for code in dic['codes']}
            act_avg = {code: sum(temp[code]) / wk_wknd['total'] for code in dic['codes']}
            for code, value in act_avg.items():
                new_line[f'avg_{code_name[code]}_min'] = round(value / epm, 2)
                new_line[f'avg_{code_name[code]}_pct'] = round(value / epd * 100, 2)

    if 'ait' in var.keys():
        if no_data:
            new_line[f'avg_ait'] = None
        else:
            temp = sum(var['ait'][day]['total'] for day in index.keys()) / wk_wknd['total']
            new_line[f'avg_ait'] = round(temp, 2)

    if 'bout' in var.keys():
        if no_data:
            for code in bout_codes:
                for nr in range(len(list(var['bout'].values())[0]['total'][code])):
                    new_line[f'avg_{code_name[code]}_bout_c{nr + 1}'] = None
        else:
            temp = {code: [var['bout'][day]['total'][code] for day in index.keys()] for code in bout_codes}
            for code, lists in temp.items():
                temp[code] = [round(sum(x) / wk_wknd['total'], 2) for x in zip(*lists)]
            for code, values in temp.items():
                for nr, val in enumerate(values):
                    new_line[f'avg_{code_name[code]}_bout_c{nr + 1}'] = val


def wk_wknd_variables(new_line, var, index, date_info, wk_wknd, epm, epd, code_name, chosen_var, bout_codes):
    """Calculate weekday and weekend averages for activity, AIT, and bout variables."""
    for key, dic in chosen_var.items():
        temp = {'wk': {}, 'wknd': {}}
        for day in index.keys():
            target = 'wknd' if date_info[day]['day_nr'] in [6, 7] else 'wk'
            for code in dic['codes']:
                temp[target].setdefault(code, []).append(var[key][day][code]['total'])

        for key2 in ['wk', 'wknd']:
            if wk_wknd[key2] == 0:
                for code in dic['codes']:
                    new_line[f'avg_{key2}_{code_name[code]}_min'] = None
                    new_line[f'avg_{key2}_{code_name[code]}_pct'] = None
            else:
                for code2, inner_list in temp[key2].items():
                    new_line[f'avg_{key2}_{code_name[code2]}_min'] = round(sum(inner_list) / wk_wknd[key2] / epm, 2)
                    new_line[f'avg_{key2}_{code_name[code2]}_pct'] = round(sum(inner_list) / wk_wknd[key2] / epd * 100, 2)

    if 'ait' in var.keys():
        temp = {'wk': [], 'wknd': []}
        for day in index.keys():
            temp['wknd' if date_info[day]['day_nr'] in [6, 7] else 'wk'].append(var['ait'][day]['total'])

        for key in ['wk', 'wknd']:
            if wk_wknd[key] == 0:
                new_line[f'avg_{key}_ait'] = None
            else:
                new_line[f'avg_{key}_ait'] = round(sum(temp[key]) / wk_wknd[key], 2)

    if 'bout' in var.keys():
        bouts_ave = {}
        for day in index.keys():
            target_dict = bouts_ave.setdefault('wknd' if date_info[day]['day_nr'] in [6, 7] else 'wk', {})
            for code in bout_codes:
                target_dict.setdefault(code, []).append(var['bout'][day]['total'][code])

        num_cats = len(list(var['bout'].values())[0]['total'][bout_codes[0]])
        for key in ['wk', 'wknd']:
            if wk_wknd[key] == 0:
                for code in bout_codes:
                    for nr in range(num_cats):
                        new_line[f'avg_{key}_{code_name[code]}_bout_c{nr + 1}'] = None
            elif key in bouts_ave:
                for code, lists in bouts_ave[key].items():
                    bouts_ave[key][code] = [round(sum(x) / wk_wknd[key], 2) for x in zip(*lists)]
                for code, values in bouts_ave[key].items():
                    for nr, val in enumerate(values):
                        new_line[f'avg_{key}_{code_name[code]}_bout_c{nr + 1}'] = val


def daily_variables(new_line, var, date_info, code_name, epd):
    """Write per-day date info, non-wear, activity, AIT, and bout variables."""
    for day, info in date_info.items():
        new_line[f'day{day}_nr'] = day
        new_line[f'day{day}_date'] = info['date']
        new_line[f'day{day}_wkday_nr'] = info['day_nr']
        new_line[f'day{day}_wkday_str'] = info['day_str']
        new_line[f'day{day}_length_min'] = info['length_epoch']
        new_line[f'day{day}_length_pct'] = round(info['length_epoch'] / epd * 100, 2) if epd > 0 else None

        if 'nw' in var.keys():
            for key, value in var['nw'][day].items():
                new_line[f'day{day}_nw_code_{key}'] = value

        for var_type in ['ai', 'act', 'walk']:
            if var_type in var.keys():
                for code, values in var[var_type][day].items():
                    new_line[f'day{day}_total_{code_name[code]}'] = values['total']
                    if 'normal' in values.keys():
                        new_line[f'day{day}_normal_{code_name[code]}'] = values['normal']
                        new_line[f'day{day}_other_{code_name[code]}'] = values['ot']

        if 'ait' in var.keys():
            new_line[f'day{day}_total_ait'] = var['ait'][day]['total']
            if 'normal' in var['ait'][day].keys():
                new_line[f'day{day}_normal_ait'] = var['ait'][day]['normal']
                new_line[f'day{day}_other_ait'] = var['ait'][day]['ot']

        if 'bout' in var.keys():
            for key in ['total', 'normal', 'other']:
                if key in var['bout'][day].keys():
                    for code, values in var['bout'][day][key].items():
                        for nr, val in enumerate(values):
                            new_line[f'day{day}_{code_name[code]}_{key}_bouts_c{nr + 1}'] = val


def other_time_variables(new_line, df, wrk_index, ot_date_info, code_name, chosen_var, bout_codes, settings, epm):
    """Write per-shift datetime, activity, walking intensity, non-wear, AIT, and bout variables."""
    if not wrk_index:
        return

    time_col = settings['time_column']
    walk_codes = settings['walk_codes']
    walk_column = settings['walk_column']
    nw_column = settings['nw_column']
    nw_codes = settings['nw_codes']

    for shift, (start, end) in wrk_index.items():
        epochs = end - start
        prefix = f'ot{shift}'

        start_datetime = datetime.strptime(df[time_col][start][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
        end_datetime = datetime.strptime(df[time_col][end - 1][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
        new_line[f'{prefix}_nr'] = shift
        new_line[f'{prefix}_start_datetime'] = start_datetime
        new_line[f'{prefix}_end_datetime'] = end_datetime
        new_line[f'{prefix}_start_wkday_nr'] = ot_date_info[shift]['day_nr']
        new_line[f'{prefix}_start_wkday_str'] = ot_date_info[shift]['day_str']
        new_line[f'{prefix}_epochs'] = epochs
        new_line[f'{prefix}_min'] = epochs / epm

        for key, dic in chosen_var.items():
            for code in dic['codes']:
                count = count_codes(df, start, end, dic['column'], code)
                new_line[f'{prefix}_{code_name[code]}_min'] = round(count / epm, 2)
                new_line[f'{prefix}_{code_name[code]}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None

        walk_total = sum(count_codes(df, start, end, walk_column, c) for c in walk_codes)
        for code in walk_codes:
            count = count_codes(df, start, end, walk_column, code)
            new_line[f'{prefix}_walk{code_name[code]}_min'] = round(count / epm, 2)
            new_line[f'{prefix}_walk{code_name[code]}_pct'] = round(count / walk_total * 100, 2) if walk_total > 0 else None

        for code in nw_codes:
            count = count_codes(df, start, end, nw_column, code)
            new_line[f'{prefix}_nw_code_{code}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None

        if settings['ait_variables']:
            new_line[f'{prefix}_ait'] = calculate_transitions(df, start, end, settings['ai_column'])

        if settings['bout_variables']:
            bouts = count_bouts(df, start, end, epm, settings)
            for code in bout_codes:
                for cat, val in enumerate(bouts[code]):
                    new_line[f'{prefix}_{code_name[code]}_bout_c{cat + 1}'] = val


def between_time_variables(new_line, df, wrk_index, code_name, chosen_var, bout_codes, settings, epm, epd):
    """Write modified shift and between-ot datetime, activity, walking, non-wear, AIT, and bout variables."""
    if not wrk_index:
        return

    modified_shifts, between_index = get_between_ot(wrk_index, epm, epd, len(df),
                                                    settings.get('min_shift_minutes', 60))

    time_col = settings['time_column']
    walk_codes = settings['walk_codes']
    walk_column = settings['walk_column']
    nw_column = settings['nw_column']
    nw_codes = settings['nw_codes']

    # Write modified shift variables (after min_shift filtering and adjacent merging)
    for shift, (start, end) in modified_shifts.items():
        epochs = end - start
        prefix = f'mod_ot{shift}'

        start_datetime = datetime.strptime(df[time_col][start][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
        end_datetime = datetime.strptime(df[time_col][end - 1][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
        new_line[f'{prefix}_nr'] = shift
        new_line[f'{prefix}_start_datetime'] = start_datetime
        new_line[f'{prefix}_end_datetime'] = end_datetime
        new_line[f'{prefix}_start_wkday_nr'] = datetime.strptime(
            df[time_col][start][:10], "%Y-%m-%d").weekday() + 1
        new_line[f'{prefix}_start_wkday_str'] = datetime.strptime(
            df[time_col][start][:10], "%Y-%m-%d").strftime('%A')
        new_line[f'{prefix}_epochs'] = epochs
        new_line[f'{prefix}_min'] = epochs / epm

        for key2, dic in chosen_var.items():
            for code in dic['codes']:
                count = count_codes(df, start, end, dic['column'], code)
                new_line[f'{prefix}_{code_name[code]}_min'] = round(count / epm, 2)
                new_line[f'{prefix}_{code_name[code]}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None

        walk_total = sum(count_codes(df, start, end, walk_column, c) for c in walk_codes)
        for code in walk_codes:
            count = count_codes(df, start, end, walk_column, code)
            new_line[f'{prefix}_walk{code_name[code]}_min'] = round(count / epm, 2)
            new_line[f'{prefix}_walk{code_name[code]}_pct'] = round(count / walk_total * 100, 2) if walk_total > 0 else None

        for code in nw_codes:
            count = count_codes(df, start, end, nw_column, code)
            new_line[f'{prefix}_nw_code_{code}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None

        if settings['ait_variables']:
            new_line[f'{prefix}_ait'] = calculate_transitions(df, start, end, settings['ai_column'])

        if settings['bout_variables']:
            bouts = count_bouts(df, start, end, epm, settings)
            for code in bout_codes:
                for cat, val in enumerate(bouts[code]):
                    new_line[f'{prefix}_{code_name[code]}_bout_c{cat + 1}'] = val

    # Write between-ot variables
    for key in sorted(between_index.keys()):
        ranges = between_index[key]
        if not ranges:
            continue

        b_start = ranges[0][0]
        b_end = ranges[-1][1]
        epochs = sum(end - start for start, end in ranges)
        prefix = f'between{key}'

        new_line[f'{prefix}_nr'] = key
        new_line[f'{prefix}_start_datetime'] = datetime.strptime(
            df[time_col][b_start][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
        new_line[f'{prefix}_end_datetime'] = datetime.strptime(
            df[time_col][b_end - 1][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
        new_line[f'{prefix}_start_wkday_nr'] = datetime.strptime(
            df[time_col][b_start][:10], "%Y-%m-%d").weekday() + 1
        new_line[f'{prefix}_start_wkday_str'] = datetime.strptime(
            df[time_col][b_start][:10], "%Y-%m-%d").strftime('%A')
        new_line[f'{prefix}_epochs'] = epochs
        new_line[f'{prefix}_min'] = epochs / epm

        for key2, dic in chosen_var.items():
            for code in dic['codes']:
                count = sum(count_codes(df, start, end, dic['column'], code)
                            for start, end in ranges)
                new_line[f'{prefix}_{code_name[code]}_min'] = round(count / epm, 2)
                new_line[f'{prefix}_{code_name[code]}_pct'] = round(
                    count / epochs * 100, 2) if epochs > 0 else None

        walk_total = sum(count_codes(df, start, end, walk_column, c)
                         for start, end in ranges for c in walk_codes)
        for code in walk_codes:
            count = sum(count_codes(df, start, end, walk_column, code)
                        for start, end in ranges)
            new_line[f'{prefix}_walk{code_name[code]}_min'] = round(count / epm, 2)
            new_line[f'{prefix}_walk{code_name[code]}_pct'] = round(
                count / walk_total * 100, 2) if walk_total > 0 else None

        for code in nw_codes:
            count = sum(count_codes(df, start, end, nw_column, code)
                        for start, end in ranges)
            new_line[f'{prefix}_nw_code_{code}_pct'] = round(
                count / epochs * 100, 2) if epochs > 0 else None

        if settings['ait_variables']:
            new_line[f'{prefix}_ait'] = sum(
                calculate_transitions(df, start, end, settings['ai_column'])
                for start, end in ranges)

        if settings['bout_variables']:
            combined_bouts = {}
            for start, end in ranges:
                bouts = count_bouts(df, start, end, epm, settings)
                for code in bout_codes:
                    if code not in combined_bouts:
                        combined_bouts[code] = [0] * len(bouts[code])
                    combined_bouts[code] = [a + b for a, b in
                                            zip(combined_bouts[code], bouts[code])]
            for code in bout_codes:
                for cat, val in enumerate(combined_bouts.get(code, [])):
                    new_line[f'{prefix}_{code_name[code]}_bout_c{cat + 1}'] = val


def daily_variables_long(subject_id, var, date_info, code_name, epd):
    """Build long-format daily rows: one dict per day."""
    rows = []
    for day, info in date_info.items():
        row = {'subject_id': subject_id,
               'day_nr': day,
               'date': info['date'],
               'wkday_nr': info['day_nr'],
               'wkday_str': info['day_str'],
               'length_min': info['length_epoch'],
               'length_pct': round(info['length_epoch'] / epd * 100, 2) if epd > 0 else None}

        if 'nw' in var:
            for key, value in var['nw'][day].items():
                row[f'nw_code_{key}'] = value

        for var_type in ['ai', 'act', 'walk']:
            if var_type in var:
                for code, values in var[var_type][day].items():
                    row[f'total_{code_name[code]}'] = values['total']
                    if 'normal' in values:
                        row[f'normal_{code_name[code]}'] = values['normal']
                        row[f'other_{code_name[code]}'] = values['ot']

        if 'ait' in var:
            row['total_ait'] = var['ait'][day]['total']
            if 'normal' in var['ait'][day]:
                row['normal_ait'] = var['ait'][day]['normal']
                row['other_ait'] = var['ait'][day]['ot']

        if 'bout' in var:
            for key in ['total', 'normal', 'other']:
                if key in var['bout'][day]:
                    for code, values in var['bout'][day][key].items():
                        for nr, val in enumerate(values):
                            row[f'{code_name[code]}_{key}_bouts_c{nr + 1}'] = val

        rows.append(row)
    return rows


def other_time_variables_long(subject_id, df, wrk_index, ot_date_info, code_name,
                              chosen_var, bout_codes, settings, epm, epd):
    """Build long-format OT rows: one dict per shift and per between-ot section.

    Returns (ot_rows, between_rows) as two separate lists.
    """
    ot_rows = []
    between_rows = []
    if not wrk_index:
        return ot_rows, between_rows

    time_col = settings['time_column']
    walk_codes = settings['walk_codes']
    walk_column = settings['walk_column']
    nw_column = settings['nw_column']
    nw_codes = settings['nw_codes']

    for shift, (start, end) in wrk_index.items():
        row = _build_ot_row(subject_id, 'ot', shift, df, start, end, [(start, end)],
                            ot_date_info[shift]['day_nr'], ot_date_info[shift]['day_str'],
                            time_col, code_name, chosen_var, walk_codes, walk_column,
                            nw_column, nw_codes, bout_codes, settings, epm)
        ot_rows.append(row)

    if settings['between_ot_variables']:
        modified_shifts, between_index = get_between_ot(wrk_index, epm, epd, len(df),
                                                        settings.get('min_shift_minutes', 60))
        # Add modified shift rows (after min_shift filtering and adjacent merging)
        for shift, (start, end) in modified_shifts.items():
            wkday_nr = datetime.strptime(df[time_col][start][:10], "%Y-%m-%d").weekday() + 1
            wkday_str = datetime.strptime(df[time_col][start][:10], "%Y-%m-%d").strftime('%A')
            row = _build_ot_row(subject_id, 'mod_ot', shift, df, start, end, [(start, end)],
                                wkday_nr, wkday_str, time_col, code_name, chosen_var,
                                walk_codes, walk_column, nw_column, nw_codes, bout_codes,
                                settings, epm)
            between_rows.append(row)

        # Add between-ot period rows
        for key in sorted(between_index.keys()):
            ranges = between_index[key]
            if not ranges:
                continue
            b_start = ranges[0][0]
            wkday_nr = datetime.strptime(df[time_col][b_start][:10], "%Y-%m-%d").weekday() + 1
            wkday_str = datetime.strptime(df[time_col][b_start][:10], "%Y-%m-%d").strftime('%A')
            row = _build_ot_row(subject_id, 'between', key, df, ranges[0][0], ranges[-1][1],
                                ranges, wkday_nr, wkday_str, time_col, code_name, chosen_var,
                                walk_codes, walk_column, nw_column, nw_codes, bout_codes,
                                settings, epm)
            between_rows.append(row)

    return ot_rows, between_rows


def _build_ot_row(subject_id, row_type, nr, df, start_idx, end_idx, ranges,
                  wkday_nr, wkday_str, time_col, code_name, chosen_var,
                  walk_codes, walk_column, nw_column, nw_codes, bout_codes, settings, epm):
    """Build a single long-format row for an OT shift or between-ot section."""
    epochs = sum(end - start for start, end in ranges)

    start_datetime = datetime.strptime(df[time_col][start_idx][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
    end_datetime = datetime.strptime(df[time_col][end_idx - 1][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")

    row = {'subject_id': subject_id, 'type': row_type, 'nr': nr,
           'start_datetime': start_datetime, 'end_datetime': end_datetime,
           'start_wkday_nr': wkday_nr, 'start_wkday_str': wkday_str,
           'epochs': epochs, 'min': epochs / epm}

    for key, dic in chosen_var.items():
        for code in dic['codes']:
            count = sum(count_codes(df, s, e, dic['column'], code) for s, e in ranges)
            row[f'{code_name[code]}_min'] = round(count / epm, 2)
            row[f'{code_name[code]}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None

    walk_total = sum(count_codes(df, s, e, walk_column, c) for s, e in ranges for c in walk_codes)
    for code in walk_codes:
        count = sum(count_codes(df, s, e, walk_column, code) for s, e in ranges)
        row[f'walk{code_name[code]}_min'] = round(count / epm, 2)
        row[f'walk{code_name[code]}_pct'] = round(count / walk_total * 100, 2) if walk_total > 0 else None

    for code in nw_codes:
        count = sum(count_codes(df, s, e, nw_column, code) for s, e in ranges)
        row[f'nw_code_{code}_pct'] = round(count / epochs * 100, 2) if epochs > 0 else None

    if settings['ait_variables']:
        row['ait'] = sum(calculate_transitions(df, s, e, settings['ai_column']) for s, e in ranges)

    if settings['bout_variables']:
        combined_bouts = {}
        for s, e in ranges:
            bouts = count_bouts(df, s, e, epm, settings)
            for code in bout_codes:
                if code not in combined_bouts:
                    combined_bouts[code] = [0] * len(bouts[code])
                combined_bouts[code] = [a + b for a, b in zip(combined_bouts[code], bouts[code])]
        for code in bout_codes:
            for cat, val in enumerate(combined_bouts.get(code, [])):
                row[f'{code_name[code]}_bout_c{cat + 1}'] = val

    return row
