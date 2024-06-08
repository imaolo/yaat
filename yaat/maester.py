from dataclasses import dataclass
from datetime import date, time, datetime
from typing import List
import pandas as pd, pandas_market_calendars as mcal


DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'

@dataclass
class TimeRange:
    start: date
    end: date
    times: List[time]
    calendar_name: str = 'NYSE'

    def __post_init__(self):
        self.start = self.clean_date(self.start)
        self.end = self.clean_date(self.end)
        if self.start > self.end: raise RuntimeError(f"start date cannot be after end date {self.start}, {self.end}")

        if not isinstance(self.times, list): raise RuntimeError(f"times must be a list type {self.times}")
        self.times = list(map(self.clean_time, self.times))
        self.calendar = mcal.get_calendar(self.calendar_name)

        self.days = self.calendar.schedule(self.start, self.end)

        self.timestamps = self.days.index.repeat(len(self.times)) + pd.to_timedelta(self.times * len(self.days))

    @staticmethod
    def clean_date(d: date | str) -> str:
        if isinstance(d, date): return d.strftime(DATE_FORMAT)
        if isinstance(d, str): return datetime.strptime(d, DATE_FORMAT).date().strftime(DATE_FORMAT)
        raise RuntimeError(f"invalid date {d}")

    @staticmethod
    def clean_time(t: time | str) -> str:
        if isinstance(t, time):
            if t.tzinfo is not None: raise RuntimeError(f"time cannot have timezone {t}")
            return t.strftime(TIME_FORMAT)
        if isinstance(t, str): return datetime.strptime(t, TIME_FORMAT).time().strftime(TIME_FORMAT)
        raise RuntimeError(f"invalid time {t}")

