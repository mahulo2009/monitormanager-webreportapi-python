import logging
from datetime import datetime
from unittest import TestCase

from mmwebreport.retrieve.daterange import DateRangeByDate, DateRangeByMonth, DateRangeByDateRange
from mmwebreport.retrieve.retrieve import RetrieveMonitor


class TestRetrieveMonitor(TestCase):

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
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "march_2022_following_error", q_clean_cache=False)

        date_range = DateRangeByDate("1H", "2022-09-22", ("19:00:00", "19:20:10"))
        data_frame = retrieve.retrieve_summary(date_range)
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

    def test_retrive_by_date(self):
        logging.basicConfig(level=logging.INFO)
        query = \
            [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "2022-03_following_error")
        date_range = DateRangeByDate("1H", "2022-07-01", ("22:20:10", "00:23:14"))
        data_frame = retrieve.retrieve_summary(date_range)
    def test_retrive_by_date_range(self):
        logging.basicConfig(level=logging.INFO)
        query = \
            [
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "2022-03_following_error")
        date_range = DateRangeByDateRange("1H", "2022-07-01", "2022-07-02", ("22:20:10", "00:23:14"))
        data_frame = retrieve.retrieve_summary(date_range)

    def test_retrive_by_month(self):
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
                }
            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "2022-03_following_error")
        date_range = DateRangeByMonth("1H", "2022-03", ("20:00:00", "06:00:00"))
        data_frame = retrieve.retrieve_summary(date_range)

    def test_retrive(self):

        logging.basicConfig(level=logging.INFO)
        query = \
            [
                # {
                #     "component": "OE.ObservingEngine",
                #     "monitor": "currentObservingState",
                #     "epsilon": 0,
                #     "type": "magnitude"
                # },
                {
                    "component": "MACS.AzimuthAxis",
                    "monitor": "position",
                    "epsilon": 0.5,
                    "type": "monitor"
                }
                # ,
                # {
                #     "component": "MACS.AzimuthAxis",
                #     "monitor": "followingError",
                #     "epsilon": 0.00002,
                #     "type": "monitor"
                # },
                # {
                #     "component": "MACS.ElevationAxis",
                #     "monitor": "position",
                #     "epsilon": 0.5,
                #     "type": "monitor"
                # },
                # {
                #     "component": "ECS.UpperShutter",
                #     "monitor": "actualPosition",
                #     "epsilon": 0.5,
                #     "type": "monitor"
                # },
                # {
                #     "component": "ECS.DomeRotation",
                #     "monitor": "actualPosition",
                #     "epsilon": 0.5,
                #     "type": "monitor"
                # },
                # {
                #     "component": "EMCS.WeatherStation",
                #     "monitor": "meanWindSpeed",
                #     "epsilon": 0.5,
                #     "type": "monitor"
                # },
                # {
                #     "component": "EMCS.WeatherStation",
                #     "monitor": "windDirection",
                #     "epsilon": 0.5,
                #     "type": "monitor"
                # },
                # {
                #     "component": "OE.ObservingEngine",
                #     "monitor": "slowGuideErrorA",
                #     "epsilon": 4.8e-07,
                #     "type": "monitor"
                # },
                # {
                #     "component": "OE.ObservingEngine",
                #     "monitor": "slowGuideErrorB",
                #     "epsilon": 4.8e-07,
                #     "type": "monitor"
                # },
                # {
                #     "component": "M1CS.Stabilisation",
                #     "monitor": "positionerPosition",
                #     "epsilon": 1500.0,
                #     "type": "array"
                # }

            ]

        retrieve = RetrieveMonitor("calp-vwebrepo", "8081", query, "2022-03_following_error")

        #date_range = DateRangeByDateRange("1H", "2022-03-01", "2022-03-01", ("19:00:00", "07:00:00"))
        date_range = DateRangeByDate("1H", "2022-03-01",  ("19:00:00", "07:00:00"))
        data_frame = retrieve.retrieve_summary(date_range)

        print(data_frame.dtypes)