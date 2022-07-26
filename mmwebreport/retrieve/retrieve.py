import os
import io

from os.path import exists
from datetime import datetime, timedelta

import pandas as pd

from mmwebreport.core.api import Report


class RetrieveMonitor(object):
    """
    A class used to make a high level request to the Monitor Manager Web Report, with the following functionality:

        1) ...

    """

    def __init__(self, host, port, q_data_ini, q_data_end, q_query, q_name):

        self.request = Report(host, port)
        self._date_ini = q_data_ini
        self._date_end = q_data_end
        self._query = q_query
        self._path = os.path.expanduser("~") + "/.cache/webreport/" + q_name + "/"
        # self.cursor = self.request.search(q_data_ini,q_data_end,q_query)

    #todo Retrieve the data from hour to hour, to cache better.
    def retrieve_daily(self):
        """
        From date initial to date final iterate over a set of monitor making individual request inside a time interval.

        For example, iterate from date1 to date2, making individual request between time1 and time2.

        """
        date_range_ini = datetime.combine(self._date_ini, self._date_ini.time())
        # todo be carefully when the time range is in the same day, not plus 1 day.
        date_range_end = datetime.combine(self._date_ini, self._date_end.time()) + timedelta(days=1)

        while date_range_ini < self._date_end:

            for idx, monitor in enumerate(self._query):
                self._run(date_range_ini, date_range_end, monitor)

            date_range_ini = date_range_ini + timedelta(days=1)
            date_range_end = date_range_end + timedelta(days=1)

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

        #todo ERROR, first check if filter data exits, just in case it does not exits, make the query.
        data_frame = self._from_cursor_get_raw(date_ini, date_end, q_entry)
        data_frame = self._filter_similar(date_ini, date_end, q_entry, data_frame)

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
            date_ini/Component/monitor/raw/
                <Component>.<monitor>.<date_ini>.<date_end>.raw.<page>.gz

        For example:
            2022-03-01/MACS/AzimuthAxis/position/raw/
                MACS.AzimuthAxis.position.2022-03-01_23_50_00.2022-03-02_00_00_00.raw.0000.gz

        :param date_ini: The search request initial date and time.
        :param date_end: The search request end date and time.
        :param a_id: Component + . + monitor
        :param page: The current search request page

        :return: a string with the name of the file where the data will be stored (cached)
        """
        path = self._path + "/" + date_ini.strftime("%Y-%m-%d") + "/" + str(a_id).replace(".", "/") + "/raw/"
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

    #todo include the epsilon in the name of the file
    def _make_filter_file_name(self, date_ini, date_end, a_id):
        """
        Build a string with the file name for the filtered data to be stored in disk (cached). The data is filtered
        using the query epsilon parameter.

        The format will be:
            date_ini/Component/monitor/
                <Component>.<monitor>.<date_ini>.<date_end>.gz

        For example:
            2022-03-01/MACS/AzimuthAxis/position/
                MACS.AzimuthAxis.position.2022-03-01_23_50_00.2022-03-02_00_00_00.gz

        :param date_ini: The search request initial date and time.
        :param date_end: The search request end date and time.
        :param a_id: Component + . + monitor

        :return: a string with the name of the file where the data will be stored (cached)
        """
        path = self._path + "/" + date_ini.strftime("%Y-%m-%d") + "/" + str(a_id).replace(".", "/")

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

        #todo error the values are not filtered at all REVIEW THIS CODE.
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
