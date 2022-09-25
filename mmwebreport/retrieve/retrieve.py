import os
import io

import pandas as pd
import logging

from mmwebreport.core.api import Report
from mmwebreport.retrieve import cache
from mmwebreport.retrieve.processing import _remove_similar_consecutive_values, _convert, _merge_data_frames, _filter


class RetrieveMonitor(object):
    """
    A class used to make a high level request to the Monitor Manager Web Report, with the following functionality:

        - Simple interface to extract with the minimum requery information complex data.
        - Cache management to optimize the time necessary to obtain the data.
        - Pagination of the result to treat with large volume of data.
        - Filtering similar values base on epsilon value.
        - Treatment of all monitor data type in a uniform way: n-dimension monitor and enumerates.
        - Integration with Pandas to generate Data Frame to easily do data science with tabular data.
        - Integrity sanity check of the request.
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
        Given a date interval and a query entry, it creates a data frame with the result. The request result
        is cached on disk.

        :param date_ini: The search request initial date and time.
        :param date_end: The search request end date and time.
        :param q_entry: The search request query entry. For example:
            {
                "component": "MACS.AzimuthAxis",
                "monitor": "position",
                "epsilon": 0.5,
                "type": "monitor"
            }

        :return: a data frame for this request.
        """
        # Create a cache object
        a_cache = cache.CacheRaw(self._path, date_ini, date_end, self._query_name, [q_entry])
        data_frames_page = []
        # Iterate over the result pages.
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
        Given a time interval, date interval and a query entry, it creates a data frame with the result, filtered by
        similar values.

        The request to the Monitor Manager Web report is done using the time interval and cached on disk as raw data.
        The data frame returned will be inside the date range and also cached. Normally, the time interval will have
        a fixed size, making easier to reuse the data raw cached between different queries.

        :param time_interval_end: The search request initial date and time.
        :param time_interval_ini: The search request end date and time.
        :param date_ini: The search filtered request initial date and time.
        :param date_end: The search filtered  request end date and time.
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
        # Create a cache object
        a_cache = cache.CacheFiltered(self._path, date_ini, date_end, self._query_name, [q_entry])
        if not self._clean_cache and a_cache.is_data_frame_cached():
            logging.info("Retrieve raw monitor from cache")
            data_frame = a_cache.data_frame_from_cache()
        else:
            logging.info("Retrieve monitor from API call")
            data_frame = self.retrieve_raw(time_interval_ini, time_interval_end, q_entry)
            # Convert time stamp to date
            data_frame = _convert(data_frame)
            # Filter data inside time interval
            data_frame = _filter(data_frame, date_ini, date_end)
            data_frame.reset_index(drop=True, inplace=True)
            # Remove similar values using the epsilon to compare if abs(a-b) < epsilon them a == b
            if q_entry["type"] == "monitor" or q_entry["type"] == "array":
                data_frame = _remove_similar_consecutive_values(data_frame,
                                                                q_entry['component'] + "." + q_entry['monitor'],
                                                                q_entry["epsilon"])
            a_cache.data_frame_to_cache(data_frame)

        return data_frame

    def retrieve_summary_chunk(self, time_interval_ini, time_interval_end, q_date_ini, q_date_end):
        """
        Given a time interval, date interval and a query, it creates a data frame with the result, containing
        several monitors, each one filtered by similar values.

        The request to the Monitor Manager Web report is done using the time interval and cached on disk as raw data
        for each monitor. Then, the filtered samples for each monitor in the date interval is obtained from raw data,
        and also cached. Finally, a data frame will be returned with samples for each monitor, inside the data range.
        This summary will be also cached.

        :param time_interval_ini: The search request initial date and time.
        :param time_interval_end: The search request end date and time.
        :param q_date_ini: The search filtered request initial date and time.
        :param q_date_end: The search filtered  request end date and time.

        :return: a data frame for this request, filtering the similar values base on epsilon monitor param and grouping
        several monitor.
        """
        logging.info("Retrieve hourly from %s to %s", time_interval_ini, time_interval_end)

        # Create a cache object
        a_cache = cache.CacheSummaryHourly(self._path,
                                           time_interval_ini, time_interval_end,
                                           self._query_name, self._query)

        if not self._clean_cache and a_cache.is_data_frame_cached():
            logging.info("Retrieve summary hourly from cache")
            data_frame = a_cache.data_frame_from_cache()
        else:
            logging.info("Retrieve summary hourly from API call")
            # Iterate over the query entries in the query
            data_frames_hourly = []
            for idx, monitor in enumerate(self._query):
                logging.info("Retrieve %s %s %s ", time_interval_ini, time_interval_end, monitor)
                data_frame = self.retrieve_filtered(time_interval_ini, time_interval_end, q_date_ini, q_date_end,
                                                    monitor)
                data_frames_hourly.append(data_frame)
            logging.info("Merge hours %s %s ", q_date_ini, q_date_end)
            # Merge all the results
            data_frame = _merge_data_frames(data_frames_hourly)
            a_cache.data_frame_to_cache(data_frame)

        return data_frame

    def retrieve_summary(self, date_range):
        """
        Given date range it creates a data frame with the result, containing several monitors, each one filtered by
        similar values.

        It is important to notice than both date and time are interpreted as intervals and not as initial and end
        datetime.

        For example, if the input is:

            date_ini: 2022-07-01 19:00:00
            date_end: 2022-07-31 06:30:00

        The result will include all the sample between 2022 07 01 and 2022 07 31, where the time is between 19:00 and
        06:30:00 and not samples from 2022-07-01 19:00:00 to 2022-07-31 06:00:00.

        In addition, in order to optimize the time necessary to obtain the data, a different cache levels are used: raw
        data from monitor manager web report (this raw data is fixed by individual monitor and with a default chunk size
        of one hour), filtered data between the date range where similar values are excluded, summary data collapsing all
        the monitors filtered in the previous step , and the final data for the date whole range.

        The query is defined using a dictionary object as follow:

            query =
                [
                    {
                        "component": "",
                        "monitor": "",
                        "epsilon": ,
                        "type": ""
                    }
                ]

        Where:

            component:  The component name following GCS convention but replacing slash with dot. For example,
                        MACS.AzimuthAxis.

            monitor:    The monitor name following GCS convention. For example: position.

            epsilon:    If different from 0, a value used to filter consecutive similar samples from a query resutl.

            type:       The typ of monitor. It can be monitor for one-dimensional monitor, array for n-dimensional
                        monitor and magnitude for monitor of type enumerated.


        In order to clarify how this works let see and example:

        Given:

            date_ini: 2022-07-01 19:00:00
            date_end: 2022-07-01 19:00:10
            query_name: following_error_study
            query = \
                [
                    {
                        "component": "OE.ObservingEngine",
                        "monitor": "currentObservingState",
                        "epsilon": 0,
                        "type": "magnitude"
                    },
                    {
                        "component": "MACS.AzimuthAxis",
                        "monitor": "position",
                        "epsilon": 0.5,
                        "type": "monitor"
                    }
                ]

        The following structure will be created on disk:

        .
        ├── 2022-07-01
        │ └── 19
        │     ├── MACS
        │     │ └── AzimuthAxis
        │     │     └── position
        │     │         ├── 2022-07-01_19_00_00.2022-07-01_19_00_10.MACS.AzimuthAxis.position.csv.gz
        │     │         ├── 2022-07-01_19_00_00.2022-07-01_19_00_10.MACS.AzimuthAxis.position.json
        │     │         └── raw
        │     │             ├── 2022-07-01_19_00_00.2022-07-01_20_00_00.MACS.AzimuthAxis.position.raw.0000.csv.gz
        │     │             └── 2022-07-01_19_00_00.2022-07-01_20_00_00.MACS.AzimuthAxis.position.raw.0000.json
        │     ├── OE
        │     │ └── ObservingEngine
        │     │     └── currentObservingState
        │     │         ├── 2022-07-01_19_00_00.2022-07-01_19_00_10.OE.ObservingEngine.currentObservingState.csv.gz
        │     │         ├── 2022-07-01_19_00_00.2022-07-01_19_00_10.OE.ObservingEngine.currentObservingState.json
        │     │         └── raw
        │     │             ├── 2022-07-01_19_00_00.2022-07-01_20_00_00.OE.ObservingEngine.currentObservingState.raw.0000.csv.gz
        │     │             └── 2022-07-01_19_00_00.2022-07-01_20_00_00.OE.ObservingEngine.currentObservingState.raw.0000.json
        │     └── summary
        │         ├── 2022-07-01_19_00_00.2022-07-01_20_00_00.following_error_study.summary.cvs.gz
        │         └── 2022-07-01_19_00_00.2022-07-01_20_00_00.following_error_study.summary.json
        └── summary
            └── following_error_study
                ├── 2022-07-01_19_00_00.2022-07-01_19_00_10.following_error_study.summary.cvs.gz
                └── 2022-07-01_19_00_00.2022-07-01_19_00_10.following_error_study.summary.json

        In the 2022-07-01/19/MACS/AzimuthAxis/position/raw/
            2022-07-01_19_00_00.2022-07-01_20_00_00.MACS.AzimuthAxis.position.raw.0000.csv.gz file are the
        samples for one monitor where time interval chunk has been fixed to one hour inside the date interval requested
        and with one file per page.

        In the 2022-07-01/19/MACS/AzimuthAxis/position/
            2022-07-01_19_00_00.2022-07-01_20_00_00.MACS.AzimuthAxis.position.csv.gz file are the
        samples for one monitor between the time interval chunk filtered by date interval and similar values.

        In the 2022-07-01/19/summary/
            2022-07-01_19_00_00.2022-07-01_20_00_00.following_error_study.summary.cvs.gz are the
        samples for all monitors between the time interval chunk filtered by date interval and similar values.

        In the 2022-07-01/summary/
            2022-07-01_19_00_00.2022-07-01_19_00_10.following_error_study.summary.cvs.gz are all the
        samples for all monitors filtered by date interval and similar values.

        :param date_range: A data range strategy. Range can be by date, date range, week, month, year.

        :return: a data frame filtering the similar values base on epsilon monitor param and grouping several monitor.
        """
        logging.info("Retrieve Summary from with range %s", date_range)

        # Create a cache object
        a_cache = cache.CacheSummary(self._path,
                                     date_range.get_date_init(), date_range.get_date_end(),
                                     self._query_name, self._query)

        if not self._clean_cache and a_cache.is_data_frame_cached():
            logging.info("Retrieve summary hourly from cache")
            data_frame = a_cache.data_frame_from_cache()
        else:
            logging.info("Retrieve summary hourly from API call")
            data_frames_monitors = []
            time_intervals = date_range.make_interval()
            for time_interval in time_intervals:
                logging.info("Retrieve hourly from %s to %s", time_interval[0], time_interval[1])
                data_frame = self.retrieve_summary_chunk(time_interval[0], time_interval[1],
                                                         time_interval[2], time_interval[3])
                data_frames_monitors.append(data_frame)
            data_frame = _merge_data_frames(data_frames_monitors)
            a_cache.data_frame_to_cache(data_frame)
        # todo cache this configuration
        if self._fillfw:
            data_frame.fillna(method='ffill', inplace=True)
            data_frame.dropna(inplace=True)

        return data_frame

    def sanity_query_check(self, a_query):
        """
        Check if a query is well-formed:

            - Component, monitor, epsilon and type have been specified.
            - Epsilon is int of float type.
            - Type is one of the defined values: monitor, array or magnitude
            - The monitor is defined in the system and the type is coherent

        :param a_query: a query with the folling structure
            {
                "component": "MACS.AzimuthAxis",
                "monitor": "position",
                "epsilon": 0.5,
                "type": "monitor"
            }
        :return: exception in case the query is not well-formed.
        """
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