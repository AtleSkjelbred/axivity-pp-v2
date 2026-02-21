from datetime import datetime
import pandas as pd


def other_times(df, subject_id, ot_run, ot_df, time_column):
    """Process work/other time data for a subject.

    Auto-detects file format (date_time or numeric) and validates all shift
    entries, reporting issues in the QC output.

    Returns:
        (ot_index, ot_qc): ot_index maps shift numbers to [start_idx, end_idx],
        or (False, ot_qc) if no valid shifts, or (False, False) if disabled.
    """
    if not ot_run:
        return False, False

    ot_qc = {'SID': subject_id}
    warnings = []

    timestamps = pd.to_datetime(df[time_column])
    data_start = timestamps.iloc[0]
    data_end = timestamps.iloc[-1]
    ot_qc['data_start'] = data_start.strftime('%Y-%m-%d %H:%M')
    ot_qc['data_end'] = data_end.strftime('%Y-%m-%d %H:%M')

    # Find subject in work times file (handle BOM in column name)
    id_col = next((c for c in ot_df.columns if c.lstrip('\ufeff') == 'ID'), None)
    if id_col is None:
        warnings.append("Work times file has no 'ID' column")
        ot_qc['warnings'] = '; '.join(warnings)
        return False, ot_qc

    matches = ot_df.index[ot_df[id_col].astype(str) == str(subject_id)].tolist()
    ot_qc['ID_occurrences'] = len(matches)

    if len(matches) == 0:
        warnings.append('Subject not found in work times file')
        ot_qc['warnings'] = '; '.join(warnings)
        return False, ot_qc
    if len(matches) > 1:
        warnings.append(f'Subject appears {len(matches)} times in work times file, expected 1')
        ot_qc['warnings'] = '; '.join(warnings)
        return False, ot_qc

    row_idx = matches[0]

    # Detect format and parse shifts
    fmt = detect_format(ot_df)
    if fmt is None:
        warnings.append('Unable to auto-detect work times file format')
        ot_qc['warnings'] = '; '.join(warnings)
        return False, ot_qc
    ot_qc['format'] = fmt

    shifts = parse_shifts(ot_df, row_idx, fmt, warnings)
    if not shifts:
        ot_qc['no_data'] = True
        ot_qc['warnings'] = '; '.join(warnings)
        return False, ot_qc
    ot_qc['no_data'] = False

    # Validate shifts against activity data range
    validate_shifts(shifts, data_start, data_end, warnings)
    if not shifts:
        ot_qc['warnings'] = '; '.join(warnings)
        return False, ot_qc

    # Map shift datetimes to dataframe row indices
    ot_index = map_shifts_to_index(df, shifts, timestamps, warnings)
    if not ot_index:
        ot_qc['warnings'] = '; '.join(warnings)
        return False, ot_qc

    ot_qc['nr_shifts'] = len(ot_index)
    for key, (start, end) in ot_index.items():
        ot_qc[f'shift_{key}_epochs'] = end - start

    ot_index = renumber_keys(ot_index)
    ot_qc['warnings'] = '; '.join(warnings) if warnings else ''
    return ot_index, ot_qc


def detect_format(ot_df):
    """Auto-detect work times file format from column headers, with data-based fallback.

    'date_time': shifts stored as date + time strings (e.g. "04.12.2023", "07:30")
    'numeric':   shifts stored as separate day/month/year/hour/minute integer fields
    """
    cols_lower = [c.lower() for c in ot_df.columns]

    # Check individual columns to avoid false matches across column name boundaries
    has_date_time_cols = any(
        'start dato' in c or 'slutt dato' in c or
        'dato start' in c or 'dato slutt' in c or
        'start kl' in c or 'slutt kl' in c
        for c in cols_lower
    )
    if has_date_time_cols and not any('dag_' in c for c in cols_lower):
        return 'date_time'
    if any('dag_' in c or 'måned' in c or 'time_start' in c for c in cols_lower):
        return 'numeric'

    # Fallback: detect from data patterns in first few rows
    for row_idx in range(min(len(ot_df), 5)):
        row = ot_df.iloc[row_idx]
        if len(row) > 3 and pd.notna(row.iloc[3]):
            val = str(row.iloc[3]).strip()
            try:
                datetime.strptime(val, '%d.%m.%Y')
                return 'date_time'
            except ValueError:
                pass
            try:
                weekday = int(float(val))
                if 1 <= weekday <= 7 and len(row) > 5 and pd.notna(row.iloc[5]):
                    day = int(float(row.iloc[5]))
                    if 1 <= day <= 31:
                        return 'numeric'
            except (ValueError, TypeError):
                pass

    return None


def parse_shifts(ot_df, row_idx, fmt, warnings):
    """Parse all shift entries from a subject's row.

    date_time format: 3 prefix cols, then blocks of 5 (start_date, start_time, end_date, end_time, comment)
    numeric format:   4 prefix cols, then blocks of 12 (shift_nr, s_day, s_month, s_year,
                      e_day, e_month, e_year, s_hour, s_min, e_hour, e_min, comment)
    """
    row = ot_df.iloc[row_idx]
    shifts = {}
    shift_nr = 0

    if fmt == 'date_time':
        prefix, block, data_size = 3, 5, 4
    else:
        prefix, block, data_size = 4, 12, 10

    col = prefix
    while col + block <= len(row):
        if fmt == 'numeric':
            data = row.iloc[col + 1:col + 1 + data_size]
        else:
            data = row.iloc[col:col + data_size]

        if data.isna().all():
            break

        shift_nr += 1

        if not data.notna().all():
            missing = data.index[data.isna()].tolist()
            warnings.append(f'Shift {shift_nr}: incomplete data, missing fields: {missing}')
            col += block
            continue

        try:
            if fmt == 'date_time':
                s_date, s_time, e_date, e_time = data.values
                start = datetime.strptime(f'{s_date} {s_time}', '%d.%m.%Y %H:%M')
                end = datetime.strptime(f'{e_date} {e_time}', '%d.%m.%Y %H:%M')
            else:
                vals = [int(float(v)) for v in data.values]
                s_day, s_month, s_year, e_day, e_month, e_year, s_hour, s_min, e_hour, e_min = vals
                if s_year > 99 or e_year > 99:
                    warnings.append(f'Shift {shift_nr}: year values ({s_year}, {e_year}) look like full years, expected 2-digit')
                start = datetime(2000 + s_year, s_month, s_day, s_hour, s_min)
                end = datetime(2000 + e_year, e_month, e_day, e_hour, e_min)
            shifts[shift_nr] = [start, end]
        except (ValueError, TypeError) as e:
            warnings.append(f'Shift {shift_nr}: invalid date/time ({e})')

        col += block

    return shifts


def validate_shifts(shifts, data_start, data_end, warnings):
    """Validate shift times and remove invalid entries."""
    to_remove = []

    for nr, (start, end) in shifts.items():
        if end < start:
            warnings.append(f'Shift {nr}: end before start ({fmt_dt(end)} < {fmt_dt(start)}), removed')
            to_remove.append(nr)
        elif end == start:
            warnings.append(f'Shift {nr}: zero duration ({fmt_dt(start)}), removed')
            to_remove.append(nr)
        else:
            if (end - start).total_seconds() > 86400:
                warnings.append(f'Shift {nr}: duration exceeds 24h ({fmt_dt(start)} to {fmt_dt(end)})')
            if end <= data_start or start >= data_end:
                warnings.append(f'Shift {nr}: entirely outside data range '
                                f'({fmt_dt(start)} to {fmt_dt(end)}), removed')
                to_remove.append(nr)
            else:
                if start < data_start:
                    warnings.append(f'Shift {nr}: starts before data ({fmt_dt(start)}), will be clamped')
                if end > data_end:
                    warnings.append(f'Shift {nr}: ends after data ({fmt_dt(end)}), will be clamped')

    for key in to_remove:
        del shifts[key]

    # Check for overlapping shifts
    sorted_shifts = sorted(shifts.items(), key=lambda x: x[1][0])
    for i in range(len(sorted_shifts) - 1):
        nr1, (_, end1) = sorted_shifts[i]
        nr2, (start2, _) = sorted_shifts[i + 1]
        if end1 > start2:
            warnings.append(f'Shift {nr1} and {nr2}: overlapping time periods')


def map_shifts_to_index(df, shifts, timestamps, warnings):
    """Map validated shift datetimes to dataframe row indices."""
    ot_index = {}

    for nr, (start_dt, end_dt) in shifts.items():
        start_mask = timestamps >= pd.Timestamp(start_dt)
        end_mask = timestamps >= pd.Timestamp(end_dt)

        if not start_mask.any():
            warnings.append(f'Shift {nr}: start time not found in data, skipped')
            continue
        start_idx = df.index[start_mask][0]

        if not end_mask.any():
            end_idx = df.index[-1] + 1
        else:
            end_idx = df.index[end_mask][0]

        if end_idx <= start_idx:
            warnings.append(f'Shift {nr}: zero length after mapping to data, skipped')
            continue

        ot_index[nr] = [start_idx, end_idx]

    return ot_index


def fmt_dt(dt):
    """Format datetime for readable warning messages."""
    return dt.strftime('%d.%m.%Y %H:%M')


def renumber_keys(d):
    """Renumber dict keys to sequential 1, 2, 3..."""
    return {i: d[k] for i, k in enumerate(sorted(d.keys()), start=1)}
