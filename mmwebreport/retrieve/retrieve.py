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

    def __init__(self, host, port, q_query, q_name, q_clean_cache=False, q_fillfw=False):
        self.request = Report(host, port)
        self._query = q_query
        self._query_name = q_name
        self._path = os.path.expanduser("~") + "/.cache/webreport/monitormanagertest"
        self._clean_cache = q_clean_cache
        self._fillfw = q_fillfw

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
        a_cache = cache.CacheRaw(self._path, date_ini, date_end, self._query_name, [q_entry])

        data_frames_page = []
        cursor = self.request.search(date_ini, date_end, [q_entry])
        for page, c in enumerate(cursor):
            logging.info("Retrieve monitor from %s to %s page %s", date_ini, date_end, page)
            if not self._clean_cache and a_cache.is_data_frame_cached(page):
                logging.info("Retrieve raw monitor from cache")
                data_frame = a_cache.data_frame_from_cache(page)
            else:
                logging.info("Retrieve monitor from API call")
                data_frame = pd.read_csv(io.StringIO(c.run().to_csv()), sep=",")
                a_cache.data_frame_to_cache(data_frame, page)
            data_frames_page.append(data_frame)
        data_frame = pd.concat(data_frames_page, ignore_index=True, sort=False)

        return data_frame

    def retrieve_filtered(self, time_interval_ini, time_interval_end, date_ini, date_end, q_entry):
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
        logging.info("Retrieve monitor from %s to %s ", date_ini, date_end)

        a_cache = cache.CacheFiltered(self._path, date_ini, date_end, self._query_name, [q_entry])

        if not self._clean_cache and a_cache.is_data_frame_cached():
            logging.info("Retrieve raw monitor from cache")
            data_frame = a_cache.data_frame_from_cache()
        else:
            logging.info("Retrieve monitor from API call")
            data_frame = self.retrieve_raw(time_interval_ini, time_interval_end, q_entry)
            data_frame = _convert(data_frame)
            data_frame = _filter(data_frame, date_ini, date_end)
            data_frame.reset_index(drop=True, inplace=True)
            if q_entry["type"] == "monitor" or q_entry["type"] == "array":
                data_frame = _remove_similar_consecutive_values(data_frame,
                                                                q_entry['component'] + "." + q_entry['monitor'],
                                                                q_entry["epsilon"])
            a_cache.data_frame_to_cache(data_frame)

        return data_frame

    def retrieve_summary_hourly(self, time_interval_ini, time_interval_end, q_date_ini, q_date_end):
        """
        todo
        :return:
        """
        logging.info("Retrieve hourly from %s to %s", time_interval_ini, time_interval_end)

        a_cache = cache.CacheSummaryHourly(self._path,
                                           time_interval_ini, time_interval_end,
                                           self._query_name, self._query)

        if not self._clean_cache and a_cache.is_data_frame_cached():
            logging.info("Retrieve summary hourly from cache")

            data_frame = a_cache.data_frame_from_cache()
        else:
            logging.info("Retrieve summary hourly from API call")

            data_frames_hourly = []
            for idx, monitor in enumerate(self._query):
                logging.info("Retrieve %s %s %s ", time_interval_ini, time_interval_end, monitor)
                data_frame = self.retrieve_filtered(time_interval_ini, time_interval_end, q_date_ini, q_date_end,
                                                    monitor)
                data_frames_hourly.append(data_frame)
            logging.info("Merge hours %s %s ", q_date_ini, q_date_end)
            # todo first time executed the merge return an error review this
            data_frame = _merge_data_frames(data_frames_hourly)
            a_cache.data_frame_to_cache(data_frame)
        return data_frame

    def retrieve_summary(self, date_ini, date_end):
        """
        :param date_ini:
        :param date_end:
        :return:
        """

        logging.info("Retrieve Summary from %s to %s", date_ini, date_end)

        a_cache = cache.CacheSummary(self._path,
                                     date_ini, date_end,
                                     self._query_name, self._query)

        if not self._clean_cache and a_cache.is_data_frame_cached():
            logging.info("Retrieve summary hourly from cache")

            data_frame = a_cache.data_frame_from_cache()
        else:
            logging.info("Retrieve summary hourly from API call")

            data_frames_monitors = []
            time_intervals = _make_time_intervals(date_ini, date_end)
            for time_interval in time_intervals:
                logging.info("Retrieve hourly from %s to %s", time_interval[0], time_interval[1])
                data_frame = self.retrieve_summary_hourly(time_interval[0], time_interval[1], date_ini, date_end)
                data_frames_monitors.append(data_frame)
            data_frame = _merge_data_frames(data_frames_monitors)
            a_cache.data_frame_to_cache(data_frame)

        if self._fillfw:
            data_frame.fillna(method='ffill', inplace=True)
            data_frame.dropna(inplace=True)

        return data_frame

    def sanity_query_check(self, a_query):

        if not isinstance(a_query, (list, tuple)):
            raise "A query must be a list of monitors."
        else:
            for e in a_query:
                if not "component" in e: raise "Component not specified."
                if not "monitor" in e: raise "Monitor not specified."
                if not "epsilon" in e: raise "Epsilon not specified."
                if not "type" in e: raise "Type not specified."

                if not (type(e['epsilon']) == float or type(e['epsilon']) == int): raise "Epsilon should be float"
                if not e['type'] in ["monitor", "array", "magnitude"]:
                    raise "Not valid value for type should be: monitor, array or enum"

                try:
                    config = self.request.get_monitor_configuration(q_component=e['component'].replace('.', '/'),
                                                                    q_monitor=e['monitor'],
                                                                    q_type=e['type'])

                    if e['type'] in "array" and config["dimension_x"] == 1 and config["dimension_y"] == 1:
                        raise "Monitor dimension incorrect"
                except:
                    raise "Monitor does not exist in database."

    # todo Check if monitor if active. This can be problematic if monitor was active at some point but not now.
    # todo Inject the ID in the a_query. This mean later on it is not neccesary to ask for this value.
    # todo Inject the unit in case the user does not defined ¿for doing this i will add support for units in query?.
    # todo Inject epsilon in case the user does not defined, what happen if epsilon not in database, shall allow to not filter at all.
    # todo ¿include subsampling option, check in this case if subsampling query make sanse with sample of monitor?
    # todo ¿include range control for the values optinally? in this case populate ddbb correctly
    # todo do chache of summary by day and store informatio to know if necessary to reproduce.
    # todo Include a summary of the process of download.
    # todo include a progress bar
    # todo wildcard, all the active monitor for a device..
    # todo reemplazar los NaN por valores iguales, antes y despues....
    # todo provie a date list.
