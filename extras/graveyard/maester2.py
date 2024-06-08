from __future__ import annotations
from yaat.util import killproc, fetchjson, myprint, DEBUG
from typing import Optional, Tuple, Dict, List, Any
from pymongo import MongoClient, UpdateOne
from zoneinfo import ZoneInfo
from pathlib import Path
from subprocess import Popen, DEVNULL
from datetime import datetime, time, timedelta, date
from dataclasses import dataclass, asdict, field
import pandas_market_calendars as mcal
import atexit, functools, pymongo.errors as mongoerrs, pandas as pd

DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'

def clean_date(d: date | str) -> str:
    if isinstance(d, date): return d.strftime(DATE_FORMAT)
    if isinstance(d, str): return datetime.strptime(d, DATE_FORMAT).date().strftime(DATE_FORMAT)
    raise RuntimeError(f"invalid date {d}")

def clean_time(t: time | str) -> str:
    if isinstance(t, time):
        if t.tzinfo is not None: raise RuntimeError(f"time cannot have timezone {t}")
        return t.strftime(TIME_FORMAT)
    if isinstance(t, str): return datetime.strptime(t, TIME_FORMAT).time().strftime(TIME_FORMAT)
    raise RuntimeError(f"invalid time {t}")

def get_exchange_timestamps(start: date | str, end: date | str, times:List[time | str], calendar_name: str = 'NYSE') -> pd.DatetimeIndex:
    start, end = clean_date(start), clean_date(end)
    times = list(map(clean_time, times))

    trading_days = mcal.get_calendar(calendar_name).schedule(start, end)

    trading_days_repeated = trading_days.index.repeat(len(times))
    times_repeated = pd.to_timedelta(times * len(trading_days))

    return trading_days_repeated + times_repeated

@dataclass
class TimeRange:
    start: date
    end: date
    times: List[str]

    def __post_init__(self):
        self.start = self.clean_date(self.start)
        self.end = self.clean_date(self.end)
        self.times = list(map(self.clean_time, self.times))

    def get_exchange_timestamps(self, calendar_name: str = 'NYSE') -> pd.DatetimeIndex:
        trading_days = mcal.get_calendar(calendar_name).schedule(self.start, self.end)
        trading_days_repeated = trading_days.index.repeat(len(self.times))
        times_repeated = pd.to_timedelta(self.times * len(trading_days))
        return trading_days_repeated + times_repeated

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

@dataclass
class DateRange:
    start: datetime
    end: datetime
    freq_min: int

    def __post_init__(self):
        self.check_freq_min(self.freq_min)
        self.check_datetime(self.start)
        self.check_datetime(self.end)
        assert self.start < self.end, f"start must be before end {self.start, self.end}"

    @property
    def intervals(self) -> pd.DatetimeIndex: return pd.date_range(start=self.start, end=self.end, freq=f'{self.freq_min}min')

    @property
    def num_intervals(self) -> int: return int(((self.end - self.start).total_seconds()) // (self.freq_min * 60)) + 1

    @staticmethod
    def check_datetime(dt: datetime): assert dt.second == 0 and dt.microsecond == 0, f"datetime must have 0 second and microsend - {dt}"

    @staticmethod
    def check_freq_min(fm:int): assert fm in (1, 5, 15, 30, 60), f"{fm} is not a valid minute interval. Valid are 1, 5, 15, 30, 60."

@dataclass  
class Ticker:
    symbol: str
    datetime: datetime
    open: float
    close: float
    high: float
    low: float
    volume: Optional[int] = None

@dataclass
class SymDate:
    symbol: int
    datetime: datetime

class Maester:
    db_name: str = 'yaatdb'
    alpha_key:str='LLE2E6Y7KG1UIS8R'

    timestamps_schema: Dict = {
        'title': 'Timestamps for merging',
        'required': ['timestamp'],
        'properties': {'timestamp': {'bsonType': 'date'},
        }
    }

    # tickers_schema and Tickers dataclass must exacly agree on field name

    tickers_schema: Dict = {
        'title': 'OHCL(V) stock, currency, and crypto currency tickers (currencies in USD)',
        'required': ['symbol', 'datetime', 'open', 'close', 'high', 'low', 'volume'],
        'properties': {
            'symbol':   {'bsonType': 'string'},
            'datetime': {'bsonType': 'date'},
            'open':     {'bsonType': 'double'},
            'close':    {'bsonType': 'double'},
            'high':     {'bsonType': 'double'},
            'low':      {'bsonType': 'double'},
            'volume':   {'bsonType': ['int', 'null']}
        }
    }

    # construction

    def __new__(cls, connstr:Optional[str]=None, dbdir:Optional[Path | str]=None):
        if connstr is not None: assert dbdir is None, 'cannot specify a connection string and to start a local database'
        else:
            try:
                cls.conndb()
                assert False, 'Local database already started(1)'
            except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure): pass
        return super().__new__(cls)

    def __init__(self, connstr:Optional[str]=None, dbdir:Optional[Path | str]=None):
        # clean and store arguments
        self.connstr = connstr if connstr is not None else 'localhost:27017'
        self.dbdir = Path(dbdir) if dbdir is not None else Path('yaatdb_local')

        # connect db (None connection string means start the database - connect via localhost)
        self.dbc, self.mongo_proc = self.startlocdb(self.dbdir) if connstr is None else (self.conndb(self.connstr), None)

        # get the database from the client connection
        self.db = self.dbc[self.db_name]

        # helper
        def create_collection(name, schema):
            if name in self.db.list_collection_names(): return self.db[name]
            else: return self.db.create_collection(name, validator={'$jsonSchema': schema})

        # create the tickers collection
        self.tickers = create_collection('tickers', self.tickers_schema)
        self.tickers.create_index({'symbol':1, 'datetime':1}, unique=True)
        self.tickers.create_index({'symbol':1})
        self.tickers.create_index({'datetime':1})

        # create the interval times collection
        self.timestamps = create_collection('timestamps', self.timestamps_schema)
        self.timestamps.create_index({'timestamp':1}, unique=True)

    # timestamp and tickers

    def fill_timestamps(self, tr: TimeRange, calendar_name:str = 'NYSE') -> int:
        timestamps = tr.get_exchange_timestamps(calendar_name)
        print(type(timestamps))
        print(len(timestamps))
        # timestamps = list(timestamps)
        # sz = sys.getsizeof(timestamps)
        # sz += sum(sys.getsizeof(ts) for ts in timestamps)
        # print(sz)
        # updates = [UpdateOne((doc:={'timestamp': dt}), {'$setOnInsert': doc}, upsert=True) for dt in timestamps]
        # result = self.timestamps.bulk_write(updates)
        # print(result.inserted_count + len(result.upserted_ids))
        # return result


    def get_missing_tickers(self, tr:TimeRange, syms:List[str], calendar_name:str = 'NYSE'): pass
    

    # alpha vantage

    def get_alpha_times(self, freq_min:int, extended:bool = False):
        res = self.call_alpha(self.alpha_key, function='TIME_SERIES_INTRADAY', symbol='IBM', extended_hours=str(extended).lower(), month='2022-04')
        print(res)

    def mine_alpha(self, freq_min:int, start:date, end:date, syms:List[str]):
        # make alpha call to get times
        miss_ticks = self.get_

    @staticmethod
    def call_alpha(api_key, **kwargs) -> Dict:
        # construct the url
        url = 'https://www.alphavantage.co/query?'
        url = url + ''.join(map(lambda kv: kv[0] + '=' + str(kv[1]) + '&', kwargs.items())) + f'apikey={api_key}'
        if DEBUG: print(f"call alpha: {url}")

        # call it (with rate limit governance)
        start = None
        while start is None or (start is not None and (time.time() - start) < 62): # 75req/min
            if 'Information' not in (data:=fetchjson(url)):
                assert 'Error Message' not in data.keys(), f"{data} \n\n {url}"
                if DEBUG: myprint("called alpha", data)
                return data
            if "higher API call volume" not in data['Information']: raise RuntimeError(data)
            if start is None: start = time.time()
        raise RuntimeError(data)

    # database

    @classmethod
    def conndb(cls, url:Optional[str]=None) -> MongoClient:
        (c := MongoClient(url, serverSelectionTimeoutMS=5000))['admin'].command('ping')
        return c

    @classmethod
    def startlocdb(cls, dbdir:Path) -> Tuple[MongoClient, Popen]:
        dbdir.mkdir(parents=True, exist_ok=True)
        try: 
            cls.conndb()
            assert False, 'Local database already started(2)'
        except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure):
            try: mongo_proc = Popen(['mongod', '--dbpath', str(dbdir.absolute())], stdout=DEVNULL, stderr=DEVNULL) # NOTE: --logpath
            except Exception as e: print(f"mongod failed: {e}"); raise
            atexit.register(functools.partial(killproc, mongo_proc))
            return cls.conndb(), mongo_proc

    def __del__(self):
        if hasattr(self, 'mongo_proc') and self.mongo_proc is not None: killproc(self.mongo_proc)

    # @staticmethod
    # def get_dr_match_agg_stage(dr:DateRange) -> Dict:
    #     return {'$match': {
    #             'datetime': {'$gte': dr.start, '$lte': dr.end}},
    #             '$expr': {'$and': [
    #                 {'$eq': [{'$second': '$datetime'}, 0]},
    #                 {'$in': [{'$minute': '$datetime'}, list(range(0, 60, dr.freq_min))]}
    #             ]}
    #     }