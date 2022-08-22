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

#def _remove_similar_consecutive_values(data_frame, monitor_name, epsilon):
    """
    Give a data frame it removes the values consecutive that are similar.

    :param data_frame: a data frame with value to be filtered
    :param monitor_name: a monitor to be filtered in the data frame
    :param epsilon: the epsilon to check if two values are similar

    :return: a data frame where similar values, base on epsilon, were removed.
    """
    # if data_frame.size > 0:
    #     to_remove = []
    #
    #     pivot = data_frame[monitor_name][0]
    #     for idx, row in data_frame.iterrows():
    #         if idx == 0:
    #             continue
    #         if abs(pivot - row[monitor_name]) < epsilon:
    #             to_remove.append(idx)
    #         else:
    #             pivot = row[monitor_name]
    #
    #     data_frame.drop(to_remove, axis=0, inplace=True)
    #
    # return data_frame


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
        (data_frame['TimeStampLong'] < q_data_end) ]

    return data_frame


#todo method not working
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
