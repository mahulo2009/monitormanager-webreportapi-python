from datetime import datetime, timedelta

from unittest import TestCase

import pandas as pd
import logging

from mmwebreport.retrieve.processing import _remove_similar_consecutive_values, _make_time_intervals, \
    _make_time_intervals_by_day, _make_time_intervals_by_week, _make_time_intervals_by_month, \
    _make_time_intervals_by_year
from mmwebreport.retrieve.retrieve import RetrieveMonitor


class TestRetrieveMonitor(TestCase):

    # def test_remove_similar_consecutive_values(self):
    #     query = \
    #         [
    #             {
    #                 "component": "MACS.AzimuthAxis",
    #                 "monitor": "position",
    #                 "epsilon": 0.5,
    #                 "type": "monitor"
    #             }
    #         ]
    #
    #     data = [[10000, 1], [10000, 1.1], [10000, 1.2], [10000, 1.3], [10000, 1.4], [10000, 1.6]]
    #     data_frame = pd.DataFrame(data=data)
    #     data_frame = _remove_similar_consecutive_values(data_frame, "a_monitor", 0.1)
    #
    #     assert data_frame.size == 4

    def test_make_time_intervals(self):
        time_interval = _make_time_intervals(datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                             datetime.strptime("2022-07-01 23:00:00", "%Y-%m-%d %H:%M:%S"))

        for ti in time_interval:
            print(ti)

    def test_retrieve_raw(self):
        logging.basicConfig(level=logging.INFO)

        query = \
            {
                "component": "MACS.AzimuthAxis",
                "monitor": "position",
                "epsilon": 0.5,
                "type": "monitor"
            }

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "study_0", q_clean_cache=False)

        data_frame = retrieve.retrieve_raw(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                           datetime.strptime("2022-07-01 19:00:10", "%Y-%m-%d %H:%M:%S"),
                                           query)

        print(data_frame)

    def test_retrieve_raw_array(self):
        logging.basicConfig(level=logging.INFO)

        query = \
            {
                "component": "M1CS.Stabilisation",
                "monitor": "positionerPosition",
                "epsilon": 0.5,
                "type": "array"
            }

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "study_0", q_clean_cache=False)

        data_frame = retrieve.retrieve_raw(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                           datetime.strptime("2022-07-01 19:00:10", "%Y-%m-%d %H:%M:%S"),
                                           query)

        print(data_frame)

    def test_retrieve_raw_enum(self):
        logging.basicConfig(level=logging.INFO)

        query = \
            {
                "component": "OE.ObservingEngine",
                "monitor": "currentObservingState",
                "type": "magnitude"
            }

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "study_0", q_clean_cache=False)

        data_frame = retrieve.retrieve_raw(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                           datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                           query)

        print(data_frame)

    def test_retrieve_filtered(self):
        logging.basicConfig(level=logging.INFO)

        query = \
            {
                "component": "MACS.AzimuthAxis",
                "monitor": "position",
                "epsilon": 0.5,
                "type": "monitor"
            }

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "study_0", q_clean_cache=False)

        data_frame = retrieve.retrieve_filtered(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                                datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                                datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                                datetime.strptime("2022-07-01 19:00:20", "%Y-%m-%d %H:%M:%S"),
                                                query)

        print(data_frame)

    def test_retrieve_filtered_array(self):
        logging.basicConfig(level=logging.INFO)

        query = \
            {
                "component": "M1CS.Stabilisation",
                "monitor": "positionerPosition",
                "epsilon": 1500,
                "type": "array"
            }

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "study_0", q_clean_cache=False)

        data_frame = retrieve.retrieve_filtered(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                                datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                                datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                                datetime.strptime("2022-07-01 19:00:10", "%Y-%m-%d %H:%M:%S"),
                                                query)

        print(data_frame)

    def test_retrieve_filtered_enum(self):
        logging.basicConfig(level=logging.INFO)

        query = \
            {
                "component": "OE.ObservingEngine",
                "monitor": "currentObservingState",
                "epsilon": 0,
                "type": "magnitude"
            }

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "study_0", q_clean_cache=False)

        data_frame = retrieve.retrieve_filtered(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                                datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                                datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                                datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                                query)

        print(data_frame)

    def test_retrieve_summary_hourly(self):
        logging.basicConfig(level=logging.INFO)

        query = \
            [

                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "followingError",
                    "epsilon": 0.00002,
                    "type": "monitor"
                },
                {
                    "component": "MACS.ElevationAxis",
                    "monitor": "followingError",
                    "epsilon": 0.00002,
                    "type": "monitor"
                },
                {
                    "component": "MACS.ElevationAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "ECS.DomeRotation",
                    "monitor": "actualPosition",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "march_2022_following_error", q_clean_cache=False)

        data_frame = retrieve.retrieve_summary_chunk(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                                     datetime.strptime("2022-07-01 19:00:10", "%Y-%m-%d %H:%M:%S"),
                                                     datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                                     datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"))

        print(data_frame)

    def test_retrieve_summary(self):
        logging.basicConfig(level=logging.INFO)

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
                },
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "followingError",
                    "epsilon": 0.00002,
                    "type": "monitor"
                },
                {
                    "component": "MACS.ElevationAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "ECS.UpperShutter",
                    "monitor": "actualPosition",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "ECS.DomeRotation",
                    "monitor": "actualPosition",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "EMCS.WeatherStation",
                    "monitor": "meanWindSpeed",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "EMCS.WeatherStation",
                    "monitor": "windDirection",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "OE.ObservingEngine",
                    "monitor": "slowGuideErrorA",
                    "epsilon": 4.8e-07,
                    "type": "monitor"
                },
                {
                    "component": "OE.ObservingEngine",
                    "monitor": "slowGuideErrorB",
                    "epsilon": 4.8e-07,
                    "type": "monitor"
                },
                {
                    "component": "M1CS.Stabilisation",
                    "monitor": "positionerPosition",
                    "epsilon": 1500.0,
                    "type": "array"
                }

            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "march_2022_following_error", q_clean_cache=False)

        data_frame = retrieve.retrieve_summary(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                               datetime.strptime("2022-07-01 19:00:10", "%Y-%m-%d %H:%M:%S"))

        print(data_frame)

    def test_sanity_check(self):
        logging.basicConfig(level=logging.INFO)

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
                },
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "followingError",
                    "epsilon": 0.00002,
                    "type": "monitor"
                },
                {
                    "component": "MACS.ElevationAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "ECS.UpperShutter",
                    "monitor": "actualPosition",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "ECS.DomeRotation",
                    "monitor": "actualPosition",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "EMCS.WeatherStation",
                    "monitor": "meanWindSpeed",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "EMCS.WeatherStation",
                    "monitor": "windDirection",
                    "epsilon": 0.5,
                    "type": "monitor"
                },
                {
                    "component": "OE.ObservingEngine",
                    "monitor": "slowGuideErrorA",
                    "epsilon": 4.8e-07,
                    "type": "monitor"
                },
                {
                    "component": "OE.ObservingEngine",
                    "monitor": "slowGuideErrorB",
                    "epsilon": 4.8e-07,
                    "type": "monitor"
                },
                {
                    "component": "M1CS.Stabilisation",
                    "monitor": "positionerPosition",
                    "epsilon": 1500.0,
                    "type": "array"
                }

            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "march_2022_following_error")
        retrieve.sanity_query_check(query)

    def test_retrieve_summary_list(self):
        logging.basicConfig(level=logging.INFO)

        query = \
            [
                {
                    "component": "MEGARA.MCS.Adam6015",
                    "monitor": "temperature",
                    "epsilon": 0,
                    "type": "array"
                },
                {
                    "component": "ECM.EMCS.IndoorSensors1",
                    "monitor": "temperature",
                    "epsilon": 0,
                    "type": "array"
                },
                {
                    "component": "ECM.EMCS.StructureSensors1",
                    "monitor": "temperature",
                    "epsilon": 0,
                    "type": "array"
                }

            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "megara_temp", q_clean_cache=True, q_fillfw=True)

        data_frame = retrieve.retrieve_summary(datetime.strptime("2022-08-16 04:50:51", "%Y-%m-%d %H:%M:%S"),
                                               datetime.strptime("2022-08-16 04:50:52", "%Y-%m-%d %H:%M:%S"))

        print(data_frame)
        # todo me está devolviendo todos los valores, fuera de rango de tiempo, lo cacheado por fechas.
        # se está liando con otros summary de mismo rango de fechas....

    def test_make_time_intervals_by_date(self):
        logging.basicConfig(level=logging.INFO)

        date_range = \
            {
                "date": "2022-07-01",
                "time_interval":
                    {
                        "time_ini": "07:20:10",
                        "time_end": "03:23:14"
                    }
            }

        ti = _make_time_intervals_by_day(date_range, "30min") 

    def test_make_time_intervals_by_date_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = \
            {
                "date": "2022-07-01"
            }

        ti = _make_time_intervals_by_day(date_range, "30min") 

    def test_make_time_intervals_by_week(self):
        logging.basicConfig(level=logging.INFO)

        week = \
            {
                "date": "2022-W2",
                "time_interval":
                    {
                        "time_ini": "23:10:00",
                        "time_end": "01:00:10"
                    }
            }

        ti = _make_time_intervals_by_week(week, "30min")

    def test_make_time_intervals_by_week_no_interval(self):
        logging.basicConfig(level=logging.INFO)

        week = \
            {
                "date": "2022-W2"
            }

        ti = _make_time_intervals_by_week(week, "30min")

    def test_make_time_intervals_by_month(self):
        logging.basicConfig(level=logging.INFO)

        month = \
            {
                "date": "2022-01",
                "time_interval":
                    {
                        "time_ini": "22:00:00",
                        "time_end": "01:00:10"
                    }
            }

        ti = _make_time_intervals_by_month(month, "30min")

    def test_make_time_intervals_by_month_no_interval(self):
        logging.basicConfig(level=logging.INFO)

        month = \
            {
                "date": "2022-01",
            }

        ti = _make_time_intervals_by_month(month, "30min")

    def test_make_time_intervals_by_year(self):
        logging.basicConfig(level=logging.INFO)

        year = \
            {
                "date": "2022",
                "time_interval":
                    {
                        "time_ini": "22:01:00",
                        "time_end": "01:00:10"
                    }
            }

        ti = _make_time_intervals_by_year(year, "30min")
        print(ti)
