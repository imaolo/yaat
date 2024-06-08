from yaat.util import killproc, DEBUG
from dataclasses import dataclass
from datetime import date, time, datetime
from pymongo import MongoClient
from subprocess import Popen, DEVNULL
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import atexit, functools, pandas as pd, pymongo.errors as mongoerrs, pandas_market_calendars as mcal


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

        self.days = self.calendar.schedule(self.start, self.end).index

        self.timestamps = self.days.repeat(len(self.times)) + pd.to_timedelta(self.times * len(self.days))

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

class Maester:
    db_name: str = 'yaatdb'

    # collection schemas

    timestamps_schema: Dict = {
        'title': 'Timestamps for merging',
        'required': ['timestamp'],
        'properties': {'timestamp': {'bsonType': 'date'},
        }
    }

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
        if connstr is not None: 
            if dbdir is not None: raise RuntimeError('cannot specify a connection string and to start a local database')
        else:
            try:
                cls.conndb()
                raise RuntimeError('Local database already started(1)')
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
        if self.mongo_proc is not None: killproc(self.mongo_proc)
