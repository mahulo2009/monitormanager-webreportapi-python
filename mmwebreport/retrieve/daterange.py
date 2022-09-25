from datetime import datetime
import pandas as pd

data_time_format = "%Y-%m-%d %H:%M:%S"
week_time_format = "%G-W%V-%u"
month_time_format = "%Y-%m"
year_time_format = "%Y"
time_format = "%H:%M:%S"

#todo include docs

class DateRange(object):

    def __init__(self, frequency, time_interval=None, date_format=data_time_format):
        self._frequency = frequency
        self._intervals = time_interval
        self._time_interval = time_interval
        self._intervals = []

        self._date_ini = None
        self._date_end = None

        self._time_ini = None
        self._time_end = None

        self._date_format = date_format

    def get_date_init(self):
        return pd.Timestamp.combine(self._date_ini, self._time_ini.time())

    def get_date_end(self):
        return pd.Timestamp.combine(self._date_end, self._time_end.time())

    def _parse_date(self):
        # Parse date, for the moment date ini and date end are the same
        self._date_ini = pd.to_datetime(self._date_ini, format=self._date_format)
        self._date_end = pd.to_datetime(self._date_end, format=self._date_format)

    def _parse_time_interval(self):
        self._date_offset = pd.DateOffset(0)
        if self._time_interval:
            self._time_ini = pd.to_datetime(self._time_interval[0], format=time_format)
            self._time_end = pd.to_datetime(self._time_interval[1], format=time_format)
        else:
            self._date_offset = pd.DateOffset(1)
            self._date_end = self._date_end + self._date_offset
            self._time_ini = pd.Timestamp(
                datetime(self._date_ini.year, self._date_ini.month, self._date_ini.day, 0, 0, 0))
            self._time_end = pd.Timestamp(
                datetime(self._date_ini.year, self._date_ini.month, self._date_ini.day, 0, 0, 0))

    def _make_time_intervals_by_date_core(self, date_ini, time_ini, date_end, time_end):
        # Round init time to near chunk frequency.
        freq_time_ini = time_ini.floor(self._frequency)
        # Ceil end time to near chunk frequency
        freq_time_end = time_end.ceil(self._frequency)
        # If time init is bigger than time end we assume time end refer to next day
        if freq_time_ini > freq_time_end:
            date_end = date_end + pd.DateOffset(1)

        freq_date_ini = pd.Timestamp.combine(date_ini.date(), freq_time_ini.time())
        freq_date_end = pd.Timestamp.combine(date_end.date(), freq_time_end.time())

        date_offset = pd.offsets.Hour(1)
        if self._frequency in '1H':
            date_offset = pd.offsets.Hour(1)
        elif self._frequency in '30min':
            date_offset = pd.offsets.Minute(30)

        intervals = []
        pivot_date = freq_date_ini
        while pivot_date < freq_date_end:
            date_ini_interval = pivot_date
            date_end_interval = pivot_date + date_offset

            date_ini_interval_1 = date_ini_interval.replace(hour=time_ini.hour, minute=time_ini.minute,
                                                            second=time_ini.second)
            date_end_interval_1 = date_end_interval.replace(hour=time_end.hour, minute=time_end.minute,
                                                            second=time_end.second)

            intervals += (date_ini_interval, date_end_interval, date_ini_interval_1, date_end_interval_1)
            pivot_date += date_offset
            date_ini += date_offset

        return intervals

    def __str__(self):
        return str(self.get_date_init()) + " - " + str(self.get_date_end())


class DateRangeByDate(DateRange):
    def __init__(self, frequency, date, time_interval=None):
        super().__init__(frequency, time_interval)

        self._date_ini = date
        self._date_end = date

        self._parse_date()
        self._parse_time_interval()

    def make_interval(self):
        self._intervals.append(self._make_time_intervals_by_date_core(self._date_ini,
                                                                      self._time_ini,
                                                                      self._date_end,
                                                                      self._time_end))
        return self._intervals


class DateRangeByDateRange(DateRange):
    def __init__(self, frequency, date_ini, date_end, time_interval=None):
        super().__init__(frequency, time_interval)

        self._date_ini = date_ini
        self._date_end = date_end

        self._parse_date()
        self._parse_time_interval()

    def make_interval(self):
        date_pivot = self._date_ini
        while date_pivot < self._date_end:
            print("---")
            self._intervals.append(self._make_time_intervals_by_date_core(date_pivot,
                                                                          self._time_ini,
                                                                          date_pivot + self._date_offset,
                                                                          self._time_end))
            date_pivot += pd.DateOffset(1)

        return self._intervals


class DateRangeByWeek(DateRange):
    def __init__(self, frequency, date, time_interval=None, date_format=week_time_format):
        super().__init__(frequency, time_interval, date_format)

        self._date_ini = date
        self._date_end = date

        self._parse_date()
        self._parse_time_interval()

    def _parse_date(self):
        # Parse date, for the moment date ini and date end are the same
        self._date_ini = pd.to_datetime(self._date_ini + "-1", format=self._date_format)
        self._date_end = pd.to_datetime(self._date_end + "-1", format=self._date_format)

    def make_interval(self):
        date_pivot = self._date_ini
        for offset in range(0, 7):
            self._intervals.append(self._make_time_intervals_by_date_core(date_pivot,
                                                                          self._time_ini,
                                                                          date_pivot + self._date_offset,
                                                                          self._time_end))
            date_pivot += pd.DateOffset(1)

        return self._intervals


class DateRangeByMonth(DateRange):
    def __init__(self, frequency, date, time_interval=None, date_format=month_time_format):
        super().__init__(frequency, time_interval, date_format)

        self._date_ini = date
        self._date_end = date

        self._parse_date()
        self._parse_time_interval()

    def make_interval(self):
        date_pivot = self._date_ini
        for offset in range(0, self._date_ini.days_in_month):
            self._intervals.append(self._make_time_intervals_by_date_core(date_pivot,
                                                                          self._time_ini,
                                                                          date_pivot + self._date_offset,
                                                                          self._time_end))
            date_pivot += pd.DateOffset(1)

        return self._intervals


class DateRangeByYear(DateRange):
    def __init__(self, frequency, date, time_interval=None, date_format=year_time_format):
        super().__init__(frequency, time_interval, date_format)

        self._date_ini = date
        self._date_end = date

        self._parse_date()
        self._parse_time_interval()

    def make_interval(self):
        date_pivot = self._date_ini
        for offset in range(0, 365 + self._date_ini.is_leap_year):
            self._intervals.append(self._make_time_intervals_by_date_core(date_pivot,
                                                                          self._time_ini,
                                                                          date_pivot + self._date_offset,
                                                                          self._time_end))
            date_pivot += pd.DateOffset(1)

        return self._intervals
