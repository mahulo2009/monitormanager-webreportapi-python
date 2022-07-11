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
        description = request.search_monitor_description(datetime.strptime("2022-03-01 20:00:00", "%Y-%m-%d %H:%M:%S") ,
                                datetime.strptime("2022-03-01 20:01:00", "%Y-%m-%d %H:%M:%S") ,
                                "OE/ObservingEngine", "siderealTime")

        assert description

    def test_search_monitor(self):

        request = Report("calp-vwebrepo", "8081")
        cursor = request.search(datetime.strptime("2022-03-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                datetime.strptime("2022-03-01 20:01:00", "%Y-%m-%d %H:%M:%S"),
                                "OE/ObservingEngine", "siderealTime")

        for c in cursor:
            print(c)

        assert cursor

    def test_search_magnitude(self):

        request = Report("calp-vwebrepo", "8081")
        cursor = request.search(datetime.strptime("2022-03-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                datetime.strptime("2022-03-31 20:01:00", "%Y-%m-%d %H:%M:%S"),
                                "OE/ObservingEngine", "currentObservingState", q_type="magnitude")
        for c in cursor:
            print(c)

        assert cursor



