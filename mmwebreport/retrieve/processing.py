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
