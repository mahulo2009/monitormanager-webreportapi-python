from datetime import datetime
from unittest import TestCase

from mmwebreport.retrieve.retrieve import RetrieveMonitor


class TestRetrieveMonitor(TestCase):
    def test_run(self):

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
                }
            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081",
                                   datetime.strptime("2022-03-01 23:50:00", "%Y-%m-%d %H:%M:%S"),
                                   datetime.strptime("2022-03-02 00:00:00", "%Y-%m-%d %H:%M:%S"),
                                   query,"test1")
        retrieve.retrieve_daily()
