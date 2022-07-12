from datetime import datetime
from unittest import TestCase

from mmwebreport.core.api import Report


class TestReport(TestCase):

    def test_get_components(self):

        request = Report("calp-vwebrepo", "8081")
        components = request.get_components()

        assert len(components) > 0

    def test_get_component(self):

        request = Report("calp-vwebrepo", "8081")
        component = request.get_component("OE/ObservingEngine")

        assert component

    def test_get_monitor(self):

        request = Report("calp-vwebrepo", "8081")
        monitor = request.get_monitor("OE/ObservingEngine", "siderealTime")

        assert monitor

    def test_search_description(self):

        request = Report("calp-vwebrepo", "8081")
        description = request.search_description(datetime.strptime("2022-03-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                                 datetime.strptime("2022-03-01 20:01:00", "%Y-%m-%d %H:%M:%S"),
                                "OE/ObservingEngine", "siderealTime")

        assert description

    def test_search_monitor(self):

        request = Report("calp-vwebrepo", "8081")
        cursor = request._search(datetime.strptime("2022-03-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                datetime.strptime("2022-03-01 20:01:00", "%Y-%m-%d %H:%M:%S"),
                                "OE/ObservingEngine", "siderealTime")

        for c in cursor:
            print(c)

        assert cursor

    def test_search_magnitude(self):

        request = Report("calp-vwebrepo", "8081")
        cursor = request._search(datetime.strptime("2022-03-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                datetime.strptime("2022-03-01 21:00:00", "%Y-%m-%d %H:%M:%S"),
                                "OE/ObservingEngine", "currentObservingState", q_type="magnitude")
        for c in cursor:
            print(c)

        assert cursor

    def test_search_monitor_set(self):

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

        request = Report("calp-vwebrepo", "8081")
        cursor = request.search(datetime.strptime("2022-03-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                datetime.strptime("2022-03-01 20:00:10", "%Y-%m-%d %H:%M:%S"),
                                query)

        for c in cursor:
            print(c)

    def test_search_stored_query(self):

        request = Report("calp-vwebrepo", "8081")
        description = request.search_stored_query(datetime.strptime("2022-03-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                datetime.strptime("2022-03-31 20:01:00", "%Y-%m-%d %H:%M:%S"),
                                "Emir")
        assert description



