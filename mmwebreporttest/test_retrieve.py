from datetime import datetime
from unittest import TestCase

import pandas as pd
import logging

from mmwebreport.retrieve.processing import _remove_similar_consecutive_values, _make_time_intervals
from mmwebreport.retrieve.retrieve import RetrieveMonitor


class TestRetrieveMonitor(TestCase):

    def test_remove_similar_consecutive_values(self):
        query = \
            [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]
        retrieve = RetrieveMonitor("calp-vwebrepo", "8081",
                                   datetime.strptime("2022-03-01", "%Y-%m-%d"),
                                   datetime.strptime("2022-03-02", "%Y-%m-%d"),
                                   datetime.strptime("23:50:00", "%H:%M:%S"),
                                   datetime.strptime("00:00:00", "%H:%M:%S"),
                                   query, "test1")

        data = {'a_monitor': [1, 1.1, 1.2, 1.3, 1.4, 1.6]}
        data_frame = pd.DataFrame(data=data)
        data_frame = _remove_similar_consecutive_values(data_frame, "a_monitor", 0.1)

        assert data_frame.size == 4

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

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "study_0")

        data_frame = retrieve.retrieve_raw(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                           datetime.strptime("2022-07-01 23:00:00", "%Y-%m-%d %H:%M:%S"),
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

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "study_0")

        data_frame = retrieve.retrieve_filtered(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                                datetime.strptime("2022-07-01 23:00:00", "%Y-%m-%d %H:%M:%S"),
                                                query)

        print(data_frame)

    def test_retrieve_summary(self):
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
                }
            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "march_2022_following_error")

        data_frame = retrieve.retrieve_summary(datetime.strptime("2022-07-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
                                               datetime.strptime("2022-07-02 00:00:00", "%Y-%m-%d %H:%M:%S"))

        print(data_frame)


    def test_retrieve_raw_m1(self):

        logging.basicConfig(level=logging.INFO)

        query = \
            {
                "component": "M1CS/Stabilisation",
                "monitor": "positionerPosition",
                "epsilon": 0.5,
                "type": "array"
            }

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "study_0")

        data_frame = retrieve.retrieve_raw(datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                           datetime.strptime("2022-07-01 23:00:00", "%Y-%m-%d %H:%M:%S"),
                                           query)

        print(data_frame)
