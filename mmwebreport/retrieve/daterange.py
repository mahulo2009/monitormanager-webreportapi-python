from datetime import datetime
import pandas as pd

data_time_format = "%Y-%m-%d %H:%M:%S"
week_time_format = "%G-W%V-%u"
month_time_format = "%Y-%m"
year_time_format = "%Y"
time_format = "%H:%M:%S"


# todo include docs


class DateRange(object):

    def __init__(self, date_format=data_time_format):
        self._date_ini = None
        self._date_end = None
        self._frequency = None
        self._time_interval = None
        self._intervals = []
        self._time_ini = None
        self._time_end = None
        self._date_format = date_format
        # In case time end is less than time ini, increase the date.
        self._date_end_plus_one_day = None
        # Setup cache step
        self._date_cache_step = None

    def get_date_init(self):
        return pd.Timestamp.combine(self._date_ini, self._time_ini.time())

    def get_date_end(self):
        return pd.Timestamp.combine(self._date_end + self._date_end_plus_one_day, self._time_end.time())

    def _parse_frequency_cache(self):
        self._date_cache_step = pd.offsets.Hour(1)
        if self._frequency in '1H':
            self._date_cache_step = pd.offsets.Hour(1)
        elif self._frequency in '30min':
            self._date_cache_step = pd.offsets.Minute(30)

    def _parse_date(self):
        # Parse date, for the moment date ini and date end are the same
        self._date_ini = pd.to_datetime(self._date_ini, format=self._date_format)
        self._date_end = pd.to_datetime(self._date_end, format=self._date_format)

    def _parse_time_interval(self):
        self._date_end_plus_one_day = pd.DateOffset(0)

        if self._time_interval:
            if self._time_interval[0] > self._time_interval[1]:
                self._date_end_plus_one_day = pd.DateOffset(1)

            self._time_ini = pd.to_datetime(self._time_interval[0], format=time_format)
            self._time_end = pd.to_datetime(self._time_interval[1], format=time_format)

        else:
            self._date_end_plus_one_day = pd.DateOffset(1)

            self._time_ini = pd.Timestamp(
                datetime(self._date_ini.year, self._date_ini.month, self._date_ini.day, 0, 0, 0))
            self._time_end = pd.Timestamp(
                datetime(self._date_ini.year, self._date_ini.month, self._date_ini.day, 0, 0, 0))

    def _make_time_intervals_by_date_core(self, date_ini, time_ini, date_end, time_end):
        # Round init time to near chunk frequency.
        freq_date_ini = pd.Timestamp.combine(date_ini.date(), time_ini.floor(self._frequency).time())
        # Ceil end time to near chunk frequency
        freq_date_end = pd.Timestamp.combine(date_end.date(), time_end.ceil(self._frequency).time())

        date_time_ini = pd.Timestamp.combine(date_ini.date(), time_ini.time())
        date_time_end = pd.Timestamp.combine(date_end.date(), time_end.time())

        intervals = []
        pivot_date = freq_date_ini
        while pivot_date < freq_date_end:
            date_ini_for_cache = pivot_date
            date_end_for_cache = pivot_date + self._date_cache_step

            data_ini_interval = date_ini_for_cache if date_ini_for_cache > date_time_ini else date_time_ini
            date_end_interval = date_end_for_cache if date_end_for_cache < date_time_end else date_time_end

            intervals.append((date_ini_for_cache, date_end_for_cache,
                              data_ini_interval, date_end_interval))

            pivot_date += self._date_cache_step

        return intervals

    def make_interval(self):
        date_pivot = self._date_ini
        while date_pivot < self._date_end + self._date_end_plus_one_day:
            self._intervals += self._make_time_intervals_by_date_core(date_pivot,
                                                                      self._time_ini,
                                                                      date_pivot + self._date_end_plus_one_day,
                                                                      self._time_end)
            date_pivot += pd.DateOffset(1)

        return self._intervals

    def build(self, date_interval, frequency="1H", time_interval=None):
        self._date_ini = date_interval[0]
        self._date_end = date_interval[1 if len(date_interval) == 2 else 0]
        self._frequency = frequency
        self._time_interval = time_interval

        self._parse_date()
        self._parse_time_interval()
        self._parse_frequency_cache()

    def from_json(self, query_json):
        self.build(query_json["date_interval"],
                   query_json["frequency"],
                   query_json["time_interval"] if "time_interval" in query_json else [])

    def __str__(self):
        return str(self.get_date_init()) + " - " + str(self.get_date_end())


class DateRangeByDate(DateRange):
    def __init__(self):
        super().__init__()


class DateRangeByDateRange(DateRange):
    def __init__(self):
        super().__init__()


class DateRangeByWeek(DateRange):
    def __init__(self):
        super().__init__(week_time_format)

    def _parse_date(self):
        # Parse date, for the moment date ini and date end are the same
        self._date_ini = pd.to_datetime(self._date_ini + "-1", format=self._date_format)
        self._date_end = pd.to_datetime(self._date_end + "-1", format=self._date_format) + pd.DateOffset(days=7 - 1)


class DateRangeByMonth(DateRange):
    def __init__(self):
        super().__init__(month_time_format)

    def _parse_date(self):
        # Parse date, for the moment date ini and date end are the same
        self._date_ini = pd.to_datetime(self._date_ini + "-1", format=self._date_format)
        self._date_end = pd.to_datetime(self._date_end + "-1", format=self._date_format) + \
                         pd.DateOffset(days=self._date_ini.days_in_month - 1)


class DateRangeByYear(DateRange):
    def __init__(self):
        super().__init__(year_time_format)

    def _parse_date(self):
        # Parse date, for the moment date ini and date end are the same
        self._date_ini = pd.to_datetime(self._date_ini, format=self._date_format)
        self._date_end = pd.to_datetime(self._date_end, format=self._date_format) + \
                         pd.DateOffset(days=365 + self._date_ini.is_leap_year - 1)
