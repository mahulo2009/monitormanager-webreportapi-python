from datetime import datetime, timedelta

import pandas as pd
import logging


def _remove_similar_consecutive_values(data_frame, monitor_name, epsilon):
    if data_frame.size > 0:
        to_remove = []

        pivot = data_frame.loc[0][1:]
        for idx, row in data_frame.iterrows():
            if idx == 0:
                continue
            if ((pivot - row[1:]) ** 2).sum() ** 0.5 <= epsilon:
                to_remove.append(idx)
            else:
                pivot = row

        data_frame.drop(to_remove, axis=0, inplace=True)

    return data_frame


def _convert(data_frame):
    data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')

    return data_frame


def _merge_data_frames(data_frames):
    data_frame = data_frames[0]
    if len(data_frames) >= 2:
        data_frame = pd.merge(data_frames[0], data_frames[1], how='outer')
        for idx in range(2, len(data_frames)):
            data_frame = pd.merge(data_frame, data_frames[idx], how='outer')

    # data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')
    data_frame.sort_values(by=['TimeStampLong'], inplace=True)

    return data_frame


def _filter(data_frame, q_data_ini, q_data_end):
    data_frame = data_frame[
        (data_frame['TimeStampLong'] >= q_data_ini) &
        (data_frame['TimeStampLong'] < q_data_end)]

    return data_frame


# todo method not working
def _make_time_intervals(q_data_ini, q_data_end):
    """
    It creates a list of hourly time interval between the date initial and date final. For example:

    Given:

        2022-03-01 22:30 - 2022-03-02 01:15

    It creates the following intervals:

        2022-03-01  22:00   -   2022-03-01  23:00
        2022-03-01  23:00   -   2022-03-02  00:00
        2022-03-02  00:00   -   2022-03-02  01:00
        2022-03-02  01:00   -   2022-03-02  02:00

        2022-03-02  22:00   -   2022-03-02  23:00
        2022-03-02  23:00   -   2022-03-03  00:00
        2022-03-03  00:00   -   2022-03-03  01:00
        2022-03-03  01:00   -   2022-03-03  02:00

    :return: a list of time intervals
    """
    time_intervals = []

    num_days = (q_data_end - q_data_ini).days + 1
    for day in range(0, num_days):
        hour_pivot = q_data_ini.hour
        time_end_hour = q_data_end.hour + (
            0 if q_data_end.minute == 0 and q_data_end.second == 0 else 1)
        while (hour_pivot % 24) != time_end_hour:
            a_interval_ini = datetime(q_data_ini.year,
                                      q_data_ini.month,
                                      q_data_ini.day,
                                      hour_pivot % 24, 0, 0)
            a_interval_ini += timedelta(days=day + int(hour_pivot / 24.0))

            hour_pivot += 1

            a_interval_end = datetime(q_data_ini.year,
                                      q_data_ini.month,
                                      q_data_ini.day,
                                      hour_pivot % 24, 0, 0)
            a_interval_end += timedelta(days=day + int(hour_pivot / 24.0))

            time_intervals.append((a_interval_ini, a_interval_end))

    return time_intervals


def _make_time_intervals_by_day(date_range, chunk_feq):
    # Parse date, for the moment date ini and date end are the same
    date_ini = pd.to_datetime(date_range['date'], format="%Y-%m-%d %H:%M:%S")
    date_end = pd.to_datetime(date_range['date'], format="%Y-%m-%d %H:%M:%S")
    # Parse time.

    if 'time_interval' in date_range:
        time_ini = pd.to_datetime(date_range['time_interval']['time_ini'], format="%H:%M:%S")
        time_end = pd.to_datetime(date_range['time_interval']['time_end'], format="%H:%M:%S")
    else:
        date_end = date_end + pd.DateOffset(1)
        time_ini = pd.Timestamp(datetime(date_ini.year, date_ini.month, date_ini.day, 0, 0, 0))
        time_end = pd.Timestamp(datetime(date_ini.year, date_ini.month, date_ini.day, 0, 0, 0))

    return _make_time_intervals_by_date_core(date_ini, time_ini, date_end, time_end, chunk_feq)


def _make_time_intervals_by_day_range(date_range, chunk_feq):
    # Parse date, for the moment date ini and date end are the same
    date_ini = pd.to_datetime(date_range['date_ini'], format="%Y-%m-%d %H:%M:%S")
    date_end = pd.to_datetime(date_range['date_end'], format="%Y-%m-%d %H:%M:%S")

    date_offset = pd.DateOffset(0)
    if 'time_interval' in date_range:
        time_ini = pd.to_datetime(date_range['time_interval']['time_ini'], format="%H:%M:%S")
        time_end = pd.to_datetime(date_range['time_interval']['time_end'], format="%H:%M:%S")
    else:
        date_offset = pd.DateOffset(1)
        date_end = date_end + pd.DateOffset(1)
        time_ini = pd.Timestamp(datetime(date_ini.year, date_ini.month, date_ini.day, 0, 0, 0))
        time_end = pd.Timestamp(datetime(date_ini.year, date_ini.month, date_ini.day, 0, 0, 0))

    time_intervals = []

    date_pivot = date_ini
    while date_pivot < date_end:
        print("---")
        time_intervals += _make_time_intervals_by_date_core(date_pivot, time_ini,
                                                            date_pivot + date_offset, time_end,
                                                            chunk_feq)
        date_pivot += pd.DateOffset(1)

    return time_intervals




def _make_time_intervals_by_week(date_range, chunk_feq):
    date_ini = pd.to_datetime(date_range['date'] + "-1", format="%G-W%V-%u")

    date_offset = pd.DateOffset(0)
    if 'time_interval' in date_range:
        time_ini = pd.to_datetime(date_range['time_interval']['time_ini'], format="%H:%M:%S")
        time_end = pd.to_datetime(date_range['time_interval']['time_end'], format="%H:%M:%S")
    else:
        date_offset = pd.DateOffset(1)
        time_ini = pd.Timestamp(datetime(date_ini.year, date_ini.month, date_ini.day, 0, 0, 0))
        time_end = pd.Timestamp(datetime(date_ini.year, date_ini.month, date_ini.day, 0, 0, 0))

    time_intervals = []

    date_pivot = date_ini
    for offset in range(0, 7):
        print("---")
        time_intervals += _make_time_intervals_by_date_core(date_pivot, time_ini,
                                                            date_pivot + date_offset, time_end,
                                                            chunk_feq)
        date_pivot += pd.DateOffset(1)

    return time_intervals


def _make_time_intervals_by_month(date_range, chunk_feq):
    date_ini = pd.to_datetime(date_range['date'], format="%Y-%m")

    date_offset = pd.DateOffset(0)
    if 'time_interval' in date_range:
        time_ini = pd.to_datetime(date_range['time_interval']['time_ini'], format="%H:%M:%S")
        time_end = pd.to_datetime(date_range['time_interval']['time_end'], format="%H:%M:%S")
    else:
        date_offset = pd.DateOffset(1)
        time_ini = pd.Timestamp(datetime(date_ini.year, date_ini.month, date_ini.day, 0, 0, 0))
        time_end = pd.Timestamp(datetime(date_ini.year, date_ini.month, date_ini.day, 0, 0, 0))

    time_intervals = []
    date_pivot = date_ini
    for offset in range(0, date_ini.days_in_month):
        print("---")
        time_intervals += _make_time_intervals_by_date_core(date_pivot, time_ini,
                                                            date_pivot + date_offset, time_end,
                                                            chunk_feq)
        date_pivot += pd.DateOffset(1)

    return time_intervals


def _make_time_intervals_by_year(date_range, chunk_feq):
    date_ini = pd.to_datetime(date_range['date'], format="%Y")

    date_offset = pd.DateOffset(0)
    if 'time_interval' in date_range:
        time_ini = pd.to_datetime(date_range['time_interval']['time_ini'], format="%H:%M:%S")
        time_end = pd.to_datetime(date_range['time_interval']['time_end'], format="%H:%M:%S")
    else:
        date_offset = pd.DateOffset(1)
        time_ini = pd.Timestamp(datetime(date_ini.year, date_ini.month, date_ini.day, 0, 0, 0))
        time_end = pd.Timestamp(datetime(date_ini.year, date_ini.month, date_ini.day, 0, 0, 0))

    time_intervals = []
    date_pivot = date_ini
    for offset in range(0, 365 + date_ini.is_leap_year):
        print("---")
        time_intervals += _make_time_intervals_by_date_core(date_pivot, time_ini,
                                                            date_pivot + date_offset, time_end,
                                                            chunk_feq)
        date_pivot += pd.DateOffset(1)

    return time_intervals


def _make_time_intervals_by_date_core(date_ini, time_ini, date_end, time_end, chunk_feq):
    # Round init time to near chunk frequency.
    time_ini = time_ini.floor(chunk_feq)
    # Ceil end time to near chunk frequency
    time_end = time_end.ceil(chunk_feq)
    # If time init is bigger than time end we assume time end refer to next day
    if time_ini > time_end:
        date_end = date_end + pd.DateOffset(1)

    date_ini = pd.Timestamp.combine(date_ini.date(), time_ini.time())
    date_end = pd.Timestamp.combine(date_end.date(), time_end.time())

    date_offset = pd.offsets.Hour(1)
    if chunk_feq in '1H':
        date_offset = pd.offsets.Hour(1)
    elif chunk_feq in '30min':
        date_offset = pd.offsets.Minute(30)

    time_intervals = []

    pivot_date = date_ini
    while pivot_date < date_end:
        print(pivot_date, pivot_date + date_offset)
        time_intervals.append((pivot_date, pivot_date + date_offset))
        pivot_date += date_offset

    return time_intervals
