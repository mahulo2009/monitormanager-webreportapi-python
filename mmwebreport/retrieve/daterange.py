from datetime import datetime
import pandas as pd

data_time_format = "%Y-%m-%d %H:%M:%S"
time_format = "%H:%M:%S"


class DateRange(object):

    def __init__(self, frequency, time_interval=None):
        self._frequency = frequency
        self._intervals = time_interval
        self._time_interval = time_interval
        self._intervals = []

    def _make_time_intervals_by_date_core(self, date_ini, time_ini, date_end, time_end):
        # Round init time to near chunk frequency.
        time_ini = time_ini.floor(self._frequency)
        # Ceil end time to near chunk frequency
        time_end = time_end.ceil(self._frequency)
        # If time init is bigger than time end we assume time end refer to next day
        if time_ini > time_end:
            date_end = date_end + pd.DateOffset(1)

        date_ini = pd.Timestamp.combine(date_ini.date(), time_ini.time())
        date_end = pd.Timestamp.combine(date_end.date(), time_end.time())

        date_offset = pd.offsets.Hour(1)
        if self._frequency in '1H':
            date_offset = pd.offsets.Hour(1)
        elif self._frequency in '30min':
            date_offset = pd.offsets.Minute(30)

        pivot_date = date_ini
        while pivot_date < date_end:
            self._intervals.append((pivot_date, pivot_date + date_offset))
            pivot_date += date_offset


class DateRangeByDate(DateRange):
    def __init__(self, frequency, date, time_interval=None):
        super().__init__(frequency, time_interval)

        self._date = date

        self._date_ini = None
        self._date_end = None
        self._time_ini = None
        self._time_end = None

        self._parse_date()
        self._parse_time_interval()

    def _parse_date(self):
        # Parse date, for the moment date ini and date end are the same
        self._date_ini = pd.to_datetime(self._date, format=data_time_format)
        self._date_end = pd.to_datetime(self._date, format=data_time_format)

    def _parse_time_interval(self):
        if self._time_interval:
            self._time_ini = pd.to_datetime(self._time_interval[0], format=time_format)
            self._time_end = pd.to_datetime(self._time_interval[1], format=time_format)
        else:
            self._date_end = self._date_end + pd.DateOffset(1)
            self._time_ini = pd.Timestamp(
                datetime(self._date_ini.year, self._date_ini.month, self._date_ini.day, 0, 0, 0))
            self._time_end = pd.Timestamp(
                datetime(self._date_ini.year, self._date_ini.month, self._date_ini.day, 0, 0, 0))

    def make_interval(self):

        self._make_time_intervals_by_date_core(self._date_ini, self._time_ini, self._date_end, self._time_end)
        return self._intervals


class DateRangeByDateRange(DateRange):
    def __init__(self, frequency, date_ini, date_end, time_interval=None):
        super().__init__(frequency, time_interval)

        self._date_ini = date_ini
        self._date_end = date_end

    def _parse_date(self):
        # Parse date, for the moment date ini and date end are the same
        self._date_ini = pd.to_datetime(self._date_ini, format=data_time_format)
        self._date_end = pd.to_datetime(self._date_end, format=data_time_format)

    def _parse_time_interval(self):
        self._date_offset = pd.DateOffset(0)

        if self._time_interval:
            self._time_ini = pd.to_datetime(self._time_interval[0], format=time_format)
            self._time_end = pd.to_datetime(self._time_interval[1], format=time_format)
        else:
            self._date_offset = pd.DateOffset(1)
            self._date_end = self._date_end + pd.DateOffset(1)
            self._time_ini = pd.Timestamp(
                datetime(self._date_ini.year, self._date_ini.month, self._date_ini.day, 0, 0, 0))
            self._time_end = pd.Timestamp(
                datetime(self._date_ini.year, self._date_ini.month, self._date_ini.day, 0, 0, 0))
