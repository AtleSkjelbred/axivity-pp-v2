"""Variable calculation: averages, weekday/weekend, daily, and work-shift summaries."""

from utils.transition import calculate_transitions
from utils.activity import count_codes
from utils.bout import count_bouts
from datetime import datetime


def calculate_variables(df, new_line, index, ot_index, date_info, ot_date_info, variables, epm, epd, settings):
    """Compute all enabled summary statistics and write them into new_line."""
    temp = {'ai': ['ai_codes', 'ai_column'], 'act': ['act_codes', 'act_column'], 'walk': ['walk_codes', 'walk_column']}
    chosen_var = {key: {'codes': settings[codes], 'column': settings[column]}
                  for key, (codes, column) in temp.items() if key in variables}
    code_name = settings['code_name']
    bout_codes = settings['bout_codes']

    wk_wknd = weekday_distribution(new_line, index, date_info, epm)
    if ot_index:
        new_line[f'nr_ot'] = len(ot_index)

    if settings['average_variables']:
        average_variables(new_line, variables, index, wk_wknd, epm, epd, code_name, chosen_var, bout_codes)
    if settings['week_wknd_variables']:
        wk_wknd_variables(new_line, variables, index, date_info, wk_wknd, epm, epd, code_name, chosen_var, bout_codes)

    if settings['daily_variables']:
        daily_variables(new_line, variables, date_info, code_name, epd)
    if settings['ot_variables']:
        other_time_variables(new_line, df, ot_index, ot_date_info, code_name, chosen_var, bout_codes, settings, epm)

    return


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
    """Write per-shift datetime, activity, AIT, and bout variables."""
    if wrk_index:
        for shift, (start, end) in wrk_index.items():
            length = end - start
            time_col = settings['time_column']
            start_datetime = datetime.strptime(df[time_col][start][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
            if end in df.index:
                end_datetime = datetime.strptime(df[time_col][end][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
            else:
                end_datetime = datetime.strptime(df[time_col][end - 1][:16], "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
            new_line[f'ot{shift}_nr'] = shift
            new_line[f'ot{shift}_start_datetime'] = start_datetime
            new_line[f'ot{shift}_end_datetime'] = end_datetime
            new_line[f'ot{shift}_start_wkday_nr'] = ot_date_info[shift]['day_nr']
            new_line[f'ot{shift}_start_wkday_str'] = ot_date_info[shift]['day_str']
            new_line[f'ot{shift}_length'] = ot_date_info[shift]['length_epoch']

            if settings['ait_variables']:
                new_line[f'ot{shift}_ait'] = calculate_transitions(df, start, end, settings['ai_column'])

            for key, dic in chosen_var.items():
                for code in dic['codes']:
                    count = count_codes(df, start, end, dic['column'], code)
                    new_line[f'ot{shift}_{code_name[code]}_min'] = round(count / epm, 2)
                    new_line[f'ot{shift}_{code_name[code]}_pct'] = round(count / length * 100, 3) if length > 0 else None

            if settings['bout_variables']:
                bouts = count_bouts(df, start, end, epm, settings)
                for code in bout_codes:
                    for cat, val in enumerate(bouts[code]):
                        new_line[f'ot{shift}_{code_name[code]}_bout_c{cat + 1}'] = val
