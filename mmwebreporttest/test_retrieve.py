from datetime import datetime
from unittest import TestCase

import pandas as pd

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
        data_frame = retrieve._remove_similar_consecutive_values(data_frame, "a_monitor", 0.1)

        assert data_frame.size == 4

    def test_make_time_intervals(self):

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
                                   datetime.strptime("2022-03-31", "%Y-%m-%d"),
                                   datetime.strptime("20:00:00", "%H:%M:%S"),
                                   datetime.strptime("07:00:00", "%H:%M:%S"),
                                   query, "study_0")
        retrieve._make_time_intervals()

    def test_retrieve_hourly(self):
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

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081",
                                   datetime.strptime("2022-07-01", "%Y-%m-%d"),
                                   datetime.strptime("2022-07-03", "%Y-%m-%d"),
                                   datetime.strptime("23:15:00", "%H:%M:%S"),
                                   datetime.strptime("01:30:00", "%H:%M:%S"),
                                   query, "study_0")
        retrieve.retrieve_hourly()