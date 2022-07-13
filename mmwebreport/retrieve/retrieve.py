import os
import io

from os.path import exists
from datetime import datetime, timedelta

import pandas as pd

from mmwebreport.core.api import Report


class RetrieveMonitor(object):

    def __init__(self, host, port, q_data_ini, q_data_end, q_query, q_name):

        self.request = Report(host, port)
        self._date_ini = q_data_ini
        self._date_end = q_data_end
        self._query = q_query
        self._path = os.path.expanduser("~") + "/.cache/webreport/" + q_name + "/"
        # self.cursor = self.request.search(q_data_ini,q_data_end,q_query)

    def _make_raw_file_name(self, date_ini, date_end, a_id, page=None):

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

    def _make_filter_file_name(self, date_ini, date_end, a_id):

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

        file_name = self._make_raw_file_name(date_ini, date_end, monitor['component'] + "." + monitor['monitor'], page)

        if not exists(file_name):

            data_frame = pd.read_csv(io.StringIO(cursor), sep=",")
            data_frame.to_csv(file_name, index=False, compression='infer')
        else:
            data_frame = pd.read_csv(file_name)

        return data_frame

    def _from_cursor_get_raw(self, date_ini, date_end, monitor):

        cursor = self.request.search(date_ini, date_end, [monitor])

        data_frames_page = []
        for page, c in enumerate(cursor):  # todo cursor is executed but if file exits it should not (split execution)

            data_frame = self._from_cursor_get_raw_n_page(date_ini, date_end, monitor, page, c)
            data_frames_page.append(data_frame)

        data_frame = pd.concat(data_frames_page, ignore_index=True, sort=False)

        return data_frame

    def _remove_similar_consecutive_values(self, data_frame, monitor_name, epsilon):

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

    def _filter_similar(self, date_ini, date_end, monitor, data_frame):

        file_name = self.make_filter_file_name(date_ini, date_end, monitor['component'] + "." + monitor['monitor'])
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

    def _run(self, date_ini, date_end, monitor):

        data_frame = self._from_cursor_get_raw(date_ini, date_end, monitor)
        data_frame = self._filter_similar(date_ini, date_end, monitor, data_frame)

        return data_frame

    def retrieve_daily(self):

        date_range_ini = datetime.combine(self._date_ini, self._date_ini.time())
        # todo be carefully when the time range is in the same day, not plus 1 day.
        date_range_end = datetime.combine(self._date_ini, self._date_end.time()) + timedelta(days=1)

        while date_range_ini < self._date_end:

            for idx, monitor in enumerate(self._query):
                print(date_range_ini, date_range_end, idx, monitor)

                self._run(date_range_ini, date_range_end, monitor)

            date_range_ini = date_range_ini + timedelta(days=1)
            date_range_end = date_range_end + timedelta(days=1)
