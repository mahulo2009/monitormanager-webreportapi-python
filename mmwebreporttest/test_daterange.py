import logging
from unittest import TestCase
from datetime import datetime

from mmwebreport.retrieve import cache
from mmwebreport.retrieve.daterange import *


class Test(TestCase):

    def test_make_time_intervals_by_date(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByDate("1H", "2022-07-01", ("22:20:10", "00:23:14"))
        print(date_range.get_date_init(),date_range.get_date_end())
        tis = date_range.make_interval()
        for ti in tis:
            print(ti)


    def test_make_time_intervals_by_date_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByDate("1H", "2022-07-01")
        tis = date_range.make_interval()
        for ti in tis:
            print(ti)

    def test_make_time_intervals_by_date_range(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByDateRange("1H", "2022-07-01", "2022-07-02", ("22:20:10", "00:23:14"))
        tis = date_range.make_interval()
        for ti in tis:
            print(ti)

    def test_make_time_intervals_by_date_range_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByDateRange("1H", "2022-07-01", "2022-07-06")
        tis = date_range.make_interval()
        for ti in tis:
            print(ti)

    def test_make_time_intervals_by_week(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByWeek("1H", "2022-W2", ("22:20:10", "00:23:14"))
        tis = date_range.make_interval()
        for ti in tis:
            print(ti)

    def test_make_time_intervals_by_week_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByWeek("1H", "2022-W2")
        tis = date_range.make_interval()
        for ti in tis:
            print(ti)

    def test_make_time_intervals_by_month(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByMonth("1H", "2022-09", ("22:20:10", "00:23:14"))
        tis = date_range.make_interval()
        for ti in tis:
            print(ti)

    def test_make_time_intervals_by_month_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByMonth("1H", "2022-09")
        tis = date_range.make_interval()
        for ti in tis:
            print(ti)

    def test_make_time_intervals_by_year(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByYear("1H", "2022", ("22:20:10", "00:23:14"))
        tis = date_range.make_interval()
        for ti in tis:
            print(ti)

    def test_make_time_intervals_by_year_no_time_interval(self):
        logging.basicConfig(level=logging.INFO)

        date_range = DateRangeByYear("1H", "2022")
        tis = date_range.make_interval()
        for ti in tis:
            print(ti)

