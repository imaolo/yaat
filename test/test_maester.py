from yaat.maester import TimeRange, TIME_FORMAT, DATE_FORMAT
from datetime import date, time, timedelta
from zoneinfo import ZoneInfo
import unittest


class TestTimeRange(unittest.TestCase):
    start, end, times = date(2020, 1, 1), date(2020, 1, 1), [time(), time(1)]
    start_str, end_str, times_str = start.strftime(DATE_FORMAT), date(2020, 1, 2).strftime(DATE_FORMAT), [t.strftime(TIME_FORMAT) for t in times]

    # should probably test clean_date and clean_time too but this mostly covers it

    def test__bad_arg_types(self):
        with self.assertRaises(RuntimeError): TimeRange(1, 1, 1)
        with self.assertRaises(RuntimeError): TimeRange(self.start, 1, 1)
        with self.assertRaises(RuntimeError): TimeRange(1, self.end, 1)
        with self.assertRaises(RuntimeError): TimeRange(self.start, self.end, 1)
        with self.assertRaises(RuntimeError): TimeRange(self.start, self.end, [1])

    def test_good_arg_types(self):
        for s in (self.start, self.start_str):
            for e in (self.end, self.end_str):
                for ts in (self.times, self.times_str):
                    TimeRange(s, e, ts)

    def test_start_greater_end(self):
        with self.assertRaises(RuntimeError): TimeRange(date(2020, 1, 2), date(2020, 1, 1), self.times)

    def test_times_w_tz(self):
        with self.assertRaises(RuntimeError): TimeRange(self.start, self.end, [time(tzinfo=ZoneInfo('UTC'))])

    def test_bad_calendar_name(self):
        with self.assertRaises(RuntimeError): TimeRange(self.start, self.end, self.times, 'non existant calendar name')

    def test_days(self):
        start = date(2024, 6, 3)
        for i in range(5):
            self.assertEqual(len(TimeRange(start, start + timedelta(days=i), self.times).days), i+1) # inclusive

