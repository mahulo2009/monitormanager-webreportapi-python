import os
import io

from os.path import exists

import pandas as pd
import logging

from mmwebreport.core.api import Report
from mmwebreport.retrieve import cache
from mmwebreport.retrieve.processing import _remove_similar_consecutive_values, _convert, _merge_data_frames, _filter, \
    _make_time_intervals


class RetrieveMonitor(object):
    """
    A class used to make a high level request to the Monitor Manager Web Report, with the following functionality:
    todo
    """

    def __init__(self, host, port, q_query, q_name):
        self.request = Report(host, port)
        self._query = q_query
        self._query_name = q_name
        self._path = os.path.expanduser("~") + "/.cache/webreport/monitormanager"

    def retrieve_raw(self, date_ini, date_end, q_entry):
        """
        Given the time interval and a query entry create a data frame with the result. The request result
        is cached in disk.

        :param q_entry: todo
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
        cursor = self.request.search(date_ini, date_end, [q_entry])

        monitor_key = q_entry['component'] + "." + q_entry['monitor']

        data_frames_page = []
        for page, c in enumerate(cursor):
            logging.info("Retrieve monitor %s from %s to %s page %s", monitor_key, date_ini, date_end, page)
            path = cache.make_path_raw(self._path, date_ini, date_end, monitor_key, page)
            logging.info("Retrieve monitor path %s ", os.path.dirname(path))
            if not exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
            if exists(os.path.dirname(path) + "/query"+str(page)+".json") and \
                    cache.compare_raw_query(cache.build_query(date_ini, date_end, self._query_name, [q_entry], page),
                                            cache.read_query(os.path.dirname(path), page)):
                logging.info("Retrieve monitor from cache %s ", path)
                data_frame = pd.read_csv(path, compression='infer')
            else:
                data_frame = pd.read_csv(io.StringIO(c.run().to_csv()), sep=",")
                data_frame.to_csv(path, index=False, compression='infer')
                cache.store_query(os.path.dirname(path), date_ini, date_end, "study_0", [q_entry], page)
            data_frames_page.append(data_frame)
        data_frame = pd.concat(data_frames_page, ignore_index=True, sort=False)

        return data_frame

    def retrieve_filtered(self, date_ini, date_end, q_entry):
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
        monitor_key = q_entry['component'] + "." + q_entry['monitor']

        logging.info("Retrieve monitor %s from %s to %s ",
                     monitor_key, date_ini, date_end)

        path = cache.make_path_filtered(self._path, date_ini, date_end, monitor_key)
        logging.info("Retrieve monitor path %s ", os.path.dirname(path))
        if not exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))

        if exists(os.path.dirname(path) + "/query.json") and \
                cache.compare_filtered_query(cache.build_query(date_ini, date_end, self._query_name, [q_entry]),
                                             cache.read_query(os.path.dirname(path))):
            logging.info("Retrieve monitor from cache %s ", path)
            data_frame = pd.read_csv(path, compression='infer')
        else:
            logging.info("Retrieve monitor from API call")
            data_frame = self.retrieve_raw(date_ini, date_end, q_entry)
            if q_entry["type"] == "monitor":
                data_frame = _remove_similar_consecutive_values(data_frame,
                                                                q_entry['component'] + "." + q_entry['monitor'],
                                                                q_entry["epsilon"])
                data_frame.to_csv(path, index=False, compression='infer')
                cache.store_query(os.path.dirname(path), date_ini, date_end, "study_0", [q_entry])
            else:
                # todo enums
                pass

        return data_frame

    def retrieve_summary_hourly(self, date_ini, date_end):
        """
        todo
        :return:
        """
        logging.info("Retrieve hourly")

        logging.info("Retrieve hourly from %s to %s", date_ini, date_end)
        path = cache.make_path_summary_hourly(self._path,  date_ini, date_end, self._query_name)
        logging.info("Retrieve path %s ", os.path.dirname(path))
        if not exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        if exists(os.path.dirname(path) + "/query.json") and \
            cache.compare_summary_query(cache.build_query( date_ini, date_end, self._query_name, self._query),
                                        cache.read_query(os.path.dirname(path))):
            logging.info("Retrieve monitor from cache %s ", path)
            data_frame = pd.read_csv(path, compression='infer')
        else:
            logging.info("Retrieve monitor from api call")
            data_frames_hourly = []
            for idx, monitor in enumerate(self._query):
                logging.info("Retrieve %s %s %s ",  date_ini, date_end, monitor)
                data_frame = self.retrieve_filtered(date_ini, date_end, monitor)
                data_frame = _convert(data_frame)
                data_frame = _filter(data_frame, date_ini, date_end)
                data_frames_hourly.append(data_frame)
            logging.info("Merge hours %s %s ",  date_ini, date_end)
            data_frame = _merge_data_frames(data_frames_hourly)
            data_frame.to_csv(path, index=False, compression='infer')
            cache.store_query(os.path.dirname(path),  date_ini, date_end, "study_0", self._query)
        return data_frame

    def retrieve_summary(self, date_ini, date_end):
        logging.info("Retrieve summary")

        path = cache.make_path_summary_query(self._path, date_ini, date_end, self._query_name)
        logging.info("Retrieve path %s ", os.path.dirname(path))

        if not exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        if exists(os.path.dirname(path) + "/query.json") and \
            cache.compare_summary_query(cache.build_query(date_ini, date_end,
                                                          self._query_name, self._query),
                                        cache.read_query(os.path.dirname(path))):
            logging.info("Retrieve monitor from cache %s ", path)

            data_frame = pd.read_csv(path, compression='infer')
        else:
            logging.info("Retrieve monitor from API call")

            data_frames_monitors = []
            time_intervals = _make_time_intervals(date_ini, date_end)
            for time_interval in time_intervals:
                logging.info("Retrieve hourly from %s to %s", time_interval[0], time_interval[1])

                data_frame = self.retrieve_summary_hourly(time_interval[0], time_interval[1])
                data_frames_monitors.append(data_frame)

            data_frame = _merge_data_frames(data_frames_monitors)
            data_frame.to_csv(path, index=False, compression='infer')
            cache.store_query(os.path.dirname(path), date_ini, date_end, "study_0", self._query)

        return data_frame

    # Todo
    # check parameters integrity.
    # do chache of summary by day and store informatio to know if necessary to reproduce.
    # Include a summary of the process of download.
    # include a progress bar

    #wildcard, all the active monitor for a device..
    #all the bla bla...

    #todo reemplazar los NaN por valores iguales, antes y despues....
