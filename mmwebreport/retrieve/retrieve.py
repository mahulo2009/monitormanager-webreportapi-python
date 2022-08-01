import os
import io

from os.path import exists
from datetime import datetime, time, timedelta

import pandas as pd

from mmwebreport.core.api import Report


class RetrieveMonitor(object):
    """
    A class used to make a high level request to the Monitor Manager Web Report, with the following functionality:
    todo
    """

    def __init__(self, host, port, q_data_ini, q_data_end, q_time_ini, q_time_end, q_query, q_name):
        self.request = Report(host, port)
        self._date_ini = q_data_ini
        self._date_end = q_data_end
        self._time_ini = q_time_ini
        self._time_end = q_time_end
        self._query = q_query
        self._query_name = q_name
        self._path = os.path.expanduser("~") + "/.cache/webreport/monitormanager"

    def _run(self, date_ini, date_end, q_entry):
        """
        Given the time interval and a query entry create a data frame with the result, filtered by similar values. The
        request result is cached in disk.

        :param date_ini: The search request initial date and time.
        :param date_end: The search request end date and time.
        :param q_entry: The search request query entry.
            {
                "component": "MACS.AzimuthAxis",
                "monitor": "position",
                "epsilon": 0.5,
                "type": "monitor"
            }

        :return: a data frame for this request, filtering the similar values base on epsilon monitor param.
        """
        file_name = self._make_filter_file_name(date_ini, date_end, q_entry['component'] + "." + q_entry['monitor'])
        if not exists(file_name):
            data_frame = self._from_cursor_get_raw(date_ini, date_end, q_entry)
            if q_entry["type"] == "monitor":
                data_frame = \
                    self._remove_similar_consecutive_values(data_frame,
                                                            q_entry['component'] + "." + q_entry['monitor'],
                                                            q_entry["epsilon"])
                data_frame.to_csv(file_name, index=False, compression='infer')
            else:  # todo coping the same file for simple merge, review this.
                data_frame.to_csv(file_name, index=False, compression='infer')
        else:
            data_frame = pd.read_csv(file_name, compression='infer')

        return data_frame

    def _from_cursor_get_raw(self, date_ini, date_end, monitor):
        """
        Given the time interval and a query entry create a data frame with the result. The request result
        is cached in disk.

        :param date_ini: The search request initial date and time.
        :param date_end: The search request end date and time.
        :param monitor: The search request query entry.
            {
                "component": "MACS.AzimuthAxis",
                "monitor": "position",
                "epsilon": 0.5,
                "type": "monitor"
            }

        :return: a data frame for this request, filtering the similar values base on epsilon monitor param.
        """
        cursor = self.request.search(date_ini, date_end, [monitor])

        data_frames_page = []
        for page, c in enumerate(cursor):
            data_frame = self._from_cursor_get_raw_n_page(date_ini, date_end, monitor, page, c)
            data_frames_page.append(data_frame)

        data_frame = pd.concat(data_frames_page, ignore_index=True, sort=False)

        return data_frame


    def _make_raw_file_name(self, date_ini, date_end, a_id, page=None):
        """
        Build a string with the file name for the raw data to be stored in disk (cached)

        The format will be:
            date_ini/hour_ini/Component/monitor/raw/
                <Component>.<monitor>.<date_ini>.<date_end>.raw.<page>.gz

        For example:
            2022-03-01/20/ECS/DomeRotation/actualPosition/raw
                ECS.DomeRotation.actualPosition.2022-03-01_20_00_00.2022-03-01_21_00_00.raw.0000.gz

        :param date_ini: The search request initial date and time.
        :param date_end: The search request end date and time.
        :param a_id: Component + . + monitor
        :param page: The current search request page

        :return: a string with the name of the file where the data will be stored (cached)
        """
        path = self._path + "/" + date_ini.strftime("%Y-%m-%d") + "/" + date_ini.strftime("%H") + "/" + str(
            a_id).replace(".", "/") + "/raw/"
        if not os.path.exists(path):
            os.makedirs(path)

        file_name_monitor = str(a_id) + \
                            "." + \
                            date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            date_end.strftime("%Y-%m-%d_%H_%M_%S")
        if page is None:
            file_name_monitor = file_name_monitor + ".gz"
        else:
            file_name_monitor = file_name_monitor + ".raw." + str(page).rjust(4, '0') + ".gz"

        return path + "/" + file_name_monitor

    # todo include the epsilon in the name of the file
    def _make_filter_file_name(self, date_ini, date_end, a_id):
        """
        Build a string with the file name for the filtered data to be stored in disk (cached). The data is filtered
        using the query epsilon parameter.

        The format will be:
            date_ini/time_ini/Component/monitor/
                <Component>.<monitor>.<date_ini>.<date_end>.gz

        For example:
            2022-03-01/20/ECS/DomeRotation/actualPosition/
                ECS.DomeRotation.actualPosition.2022-03-01_20_00_00.2022-03-01_21_00_00.gz

        :param date_ini: The search request initial date and time.
        :param date_end: The search request end date and time.
        :param a_id: Component + . + monitor

        :return: a string with the name of the file where the data will be stored (cached)
        """
        path = self._path + "/" + date_ini.strftime("%Y-%m-%d") + "/" + date_ini.strftime("%H") + "/" + str(
            a_id).replace(".", "/")

        if not os.path.exists(path):
            os.makedirs(path)

        file_name_monitor = str(a_id) + \
                            "." + \
                            date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            date_end.strftime("%Y-%m-%d_%H_%M_%S")
        file_name_monitor = file_name_monitor + ".gz"

        return path + "/" + file_name_monitor

    def _from_cursor_get_raw_n_page(self, date_ini, date_end, monitor, page, cursor):
        """
        Given a query data return a data frame. If cache data does not exits, it makes a query
        to the backend.

        :param date_ini: The search request initial date and time.
        :param date_end: The search request end date and time.
        :param monitor:  The search request query entry.
            {
                "component": "MACS.AzimuthAxis",
                "monitor": "position",
                "epsilon": 0.5,
                "type": "monitor"
            }
        :param page: The current page.
        :param cursor: The cursor associated to this query.

        :return: a data frame for this request.
        """
        file_name = self._make_raw_file_name(date_ini, date_end, monitor['component'] + "." + monitor['monitor'], page)

        if not exists(file_name):
            data_frame = pd.read_csv(io.StringIO(cursor.run().to_csv()), sep=",")
            data_frame.to_csv(file_name, index=False, compression='infer')
        else:
            data_frame = pd.read_csv(file_name)

        return data_frame

    def _filter_similar(self, date_ini, date_end, monitor, data_frame):
        """
        Given a query data return a data frame. If cache data does not exits, it filters the data.

        :param date_ini: The search request initial date and time.
        :param date_end: The search request end date and time.
        :param monitor: The search request query entry.
            {
                "component": "MACS.AzimuthAxis",
                "monitor": "position",
                "epsilon": 0.5,
                "type": "monitor"
            }
        :param data_frame: Data frame to be filtered.

        :return: A data frame filtered base on query epsilon param
        """

        # todo error the values are not filtered at all REVIEW THIS CODE.
        file_name = self._make_filter_file_name(date_ini, date_end, monitor['component'] + "." + monitor['monitor'])
        if not exists(file_name):
            if monitor["type"] == "monitors":
                data_frame = \
                    self._remove_similar_consecutive_values(data_frame,
                                                            monitor['component'] + "." + monitor['monitor'],
                                                            monitor["epsilon"])
                data_frame.to_csv(file_name, index=False, compression='infer')
            else:  # todo coping the same file for simple merge, review this.
                data_frame.to_csv(file_name, index=False, compression='infer')

        return data_frame

    def _remove_similar_consecutive_values(self, data_frame, monitor_name, epsilon):
        """
        Give a data frame it removes the values consecutive that are similar.

        :param data_frame: a data frame with value to be filtered
        :param monitor_name: a monitor to be filtered in the data frame
        :param epsilon: the epsilon to check if two values are similar

        :return: a data frame where similar values, base on epsilon, were removed.
        """
        if data_frame.size > 0:
            to_remove = []

            pivot = data_frame[monitor_name][0]
            for idx, row in data_frame.iterrows():
                if idx == 0:
                    continue
                if abs(pivot - row[monitor_name]) < epsilon:
                    to_remove.append(idx)
                else:
                    pivot = row[monitor_name]

            data_frame.drop(to_remove, axis=0, inplace=True)

        return data_frame

    def _convert(self, data_frame):
        data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')

        return data_frame

    def _filter(self, data_frame):

        data_frame = data_frame[
            (data_frame['TimeStampLong'] >= datetime.combine(self._date_ini, self._time_ini.time())) &
            (data_frame['TimeStampLong'] < datetime.combine(self._date_end, self._time_end.time()))]

        return data_frame

    def _merge_data_frames(self, data_frames):

        data_frame = data_frames[0]
        if len(data_frames) >= 2:
            data_frame = pd.merge(data_frames[0], data_frames[1], how='outer')
            for idx in range(2, len(data_frames)):
                data_frame = pd.merge(data_frame, data_frames[idx], how='outer')

        # data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')
        data_frame.sort_values(by=['TimeStampLong'], inplace=True)

        return data_frame

    def _make_combined_hourly_file_name(self, date_ini, date_end):
        path = self._path + "/" \
               + date_ini.strftime("%Y-%m-%d") + "/" + date_ini.strftime("%H") \
               + "/summary"

        if not os.path.exists(path):
            os.makedirs(path)

        file_name_monitor = date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            date_end.strftime("%Y-%m-%d_%H_%M_%S")
        file_name_monitor = self._query_name + "." + "merge" + "." + file_name_monitor + ".gz"

        return path + "/" + file_name_monitor

    # todo include the epsilon in the name of the file
    def _make_combined_file_name(self, date_ini, date_end):
        """
        Build a string with the file name for summary data to be stored in disk (cached). The summary include
        all the monitor for the same hour in a query.

        The format will be:
            date_ini/time_init/summary/
                <>.merge.<date_ini>.<date_end>.gz

        For example:
            2022-03-01/20/summary/
                study_0.merge.2022-03-01_20_00_00.2022-03-01_21_00_00.gz

        :param date_ini: The search request initial date and time.
        :param date_end: The search request end date and time.

        :return: a string with the name of the file where the data will be stored (cached)
        """
        path = self._path + "/summary/"

        if not os.path.exists(path):
            os.makedirs(path)

        file_name_monitor = date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            date_end.strftime("%Y-%m-%d_%H_%M_%S")
        file_name_monitor = file_name_monitor + ".summary" + ".gz"

        return path + "/" + file_name_monitor

    def _make_time_intervals(self):
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

        num_days = (self._date_end - self._date_ini).days
        for day in range(0, num_days):
            hour_pivot = self._time_ini.hour
            time_end_hour = self._time_end.hour + (
                0 if self._time_end.minute == 0 and self._time_end.second == 0 else 1)
            while (hour_pivot % 24) != time_end_hour:
                a_interval_ini = datetime(self._date_ini.year,
                                          self._date_ini.month,
                                          self._date_ini.day,
                                          hour_pivot % 24, 0, 0)
                a_interval_ini += timedelta(days=day + int(hour_pivot / 24.0))

                hour_pivot += 1

                a_interval_end = datetime(self._date_ini.year,
                                          self._date_ini.month,
                                          self._date_ini.day,
                                          hour_pivot % 24, 0, 0)
                a_interval_end += timedelta(days=day + int(hour_pivot / 24.0))

                time_intervals.append((a_interval_ini, a_interval_end))

        return time_intervals

    def retrieve_hourly(self):
        """
        todo
        :return:
        """
        time_intervals = self._make_time_intervals()
        data_frames_monitors = []
        for time_interval in time_intervals:
            file_name = self._make_combined_hourly_file_name(time_interval[0], time_interval[1])
            print(file_name)

            if not exists(file_name):
                data_frames_hourly = []
                for idx, monitor in enumerate(self._query):
                    print("->check " + monitor['component'] + "." + monitor['monitor'])
                    data_frame = self._run(time_interval[0], time_interval[1], monitor)
                    data_frame = self._convert(data_frame)
                    data_frame = self._filter(data_frame)
                    data_frames_hourly.append(data_frame)
                print("merge " + monitor['component'] + "." + monitor['monitor'])
                data_frame = self._merge_data_frames(data_frames_hourly)
                data_frame.to_csv(file_name, index=False, compression='infer')
            else:
                data_frame = pd.read_csv(file_name, compression='infer')
            data_frames_monitors.append(data_frame)

        print("merge all")
        file_name = self._make_combined_file_name(self._date_ini, self._date_end)
        data_frame = self._merge_data_frames(data_frames_monitors)
        data_frame.to_csv(file_name, index=False, compression='infer')

