from unittest import TestCase
from datetime import datetime

from mmwebreport.retrieve import cache


class Test(TestCase):
    def test_make_path_raw(self):
        path = cache.make_path_raw("~/.cache/webreport/monitormanager",
                                   datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                   datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                   "MACS.AzimuthAxis.position", 1)

        print(path)

    def test_make_path_filtered(self):
        path = cache.make_path_filtered("~/.cache/webreport/monitormanager",
                                        datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                        datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                        "MACS.AzimuthAxis.position")

        print(path)

    def test_make_path_summary_hourly(self):
        path = cache.make_path_summary_hourly("~/.cache/webreport/monitormanager",
                                              datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                              datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                              "study_0")

        print(path)

    def test_make_path_summary_query(self):
        path = cache.make_path_summary("~/.cache/webreport/monitormanager",
                                       datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                       datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                             "study_0")

        print(path)

    def test_store_query_raw(self):
        query = \
            {
                "component": "MACS.AzimuthAxis",
                "monitor": "position",
                "epsilon": 0.5,
                "type": "monitor"
            }
        cache.store_query_raw("/tmp",
                              datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                              datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                              "study_0",
                              query,
                              0)

        pass

    def test_read_query_raw(self):
        query = \
            {
                "component": "MACS.AzimuthAxis",
                "monitor": "position",
                "epsilon": 0.5,
                "type": "monitor"
            }
        r = cache.read_query_raw("/tmp",
                                 datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                                 datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                                 query,
                                 0)

        print(r)

    def test_store_query(self):
        query = \
            [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]

        cache.store_query("/tmp/",
                          datetime.strptime("2022-07-01 19:00:00", "%Y-%m-%d %H:%M:%S"),
                          datetime.strptime("2022-07-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                          "study_0",
                          query,
                          0)

    def test_store_query_filtered(self):
        query = \
            [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]

        cache.store_query("/tmp/",
                          datetime.strptime("2022-03-01 20:13:00", "%Y-%m-%d %H:%M:%S"),
                          datetime.strptime("2022-03-01 21:11:32", "%Y-%m-%d %H:%M:%S"),
                          "study_0",
                          query)

    def test_store_query_summary_hourly(self):
        query = \
            [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]

        cache.store_query("/tmp/",
                          datetime.strptime("2022-03-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                          datetime.strptime("2022-03-01 21:00:00", "%Y-%m-%d %H:%M:%S"),
                          "study_0",
                          query)

    def test_store_query_summary(self):
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

        cache.store_query("/tmp/",
                          datetime.strptime("2022-03-01 20:00:00", "%Y-%m-%d %H:%M:%S"),
                          datetime.strptime("2022-03-01 21:00:00", "%Y-%m-%d %H:%M:%S"),
                          "study_0",
                          query)

    def test_compare_raw_query(self):
        q1 = {
            "date_ini": "2022-03-01 20_00_00",
            "date_end": "2022-03-01 21_00_00",
            "query_name": "study_0",
            "query": [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.1,
                    "type": "monitor"
                }
            ]
        }

        q2 = {
            "date_ini": "2022-03-01 20_00_00",
            "date_end": "2022-03-01 21_00_00",
            "query_name": "study_0",
            "query": [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]
        }

        assert cache.compare_raw_query(q1, q2)

    def test_compare_filtered_query(self):
        q1 = {
            "date_ini": "2022-03-01 20_00_00",
            "date_end": "2022-03-01 21_00_00",
            "query_name": "study_0",
            "query": [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]
        }

        q2 = {
            "date_ini": "2022-03-01 20_00_00",
            "date_end": "2022-03-01 21_00_00",
            "query_name": "study_0",
            "query": [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]
        }

        assert cache.compare_filtered_query(q1, q2)

    def test_compare_summary_query(self):
        q1 = {
            "date_ini": "2022-03-01 20_00_00",
            "date_end": "2022-03-01 21_00_00",
            "query_name": "study_0",
            "query": [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
                ,
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "followingError",
                    "epsilon": 0.00002,
                    "type": "monitor"
                }
            ]
        }

        q2 = {
            "date_ini": "2022-03-01 20_00_00",
            "date_end": "2022-03-01 21_00_00",
            "query_name": "study_0",
            "query": [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
                ,
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "followingError",
                    "epsilon": 0.00002,
                    "type": "monitor"
                }

            ]
        }

        assert cache.compare_summary_query(q1, q2)
