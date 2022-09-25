import logging
from unittest import TestCase
from datetime import datetime

from mmwebreport.retrieve import cache
from mmwebreport.retrieve.daterange import *


class Test(TestCase):

    def test_make_time_intervals_by_date(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByDate("30min", "2022-07-01", ("07:20:10", "03:23:14"))
        ti = date_range.make_interval()
        print(ti)

    def test_make_time_intervals_by_date_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByDate("30min", "2022-07-01")
        ti = date_range.make_interval()
        print(ti)

    def test_make_time_intervals_by_date_range(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByDateRange("30min", "2022-07-01", "2022-07-06", ("23:20:10", "00:23:14"))
        ti = date_range.make_interval()
        print(ti)

    def test_make_time_intervals_by_date_range_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByDateRange("30min", "2022-07-01", "2022-07-06")
        ti = date_range.make_interval()
        print(ti)

    def test_make_time_intervals_by_week(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByWeek("1H", "2022-W2", ("19:00:00", "19:20:10"))
        ti = date_range.make_interval()
        print(ti)

    def test_make_time_intervals_by_week_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByWeek("1H", "2022-W2")
        ti = date_range.make_interval()
        print(ti)

    def test_make_time_intervals_by_month(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByMonth("1H", "2022-09", ("19:00:00", "19:20:10"))
        ti = date_range.make_interval()
        print(ti)

    def test_make_time_intervals_by_month_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByMonth("1H", "2022-09")
        ti = date_range.make_interval()
        print(ti)

    def test_make_time_intervals_by_year(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByYear("1H", "2022", ("19:00:00", "19:20:10"))
        ti = date_range.make_interval()
        print(ti)

    def test_make_time_intervals_by_year_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByYear("1H", "2022")
        ti = date_range.make_interval()
        print(ti)
