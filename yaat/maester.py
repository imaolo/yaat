from __future__ import annotations
from yaat.util import killproc
from typing import Optional, Tuple, Dict, List, Generator
from pymongo import MongoClient
from zoneinfo import ZoneInfo
from pathlib import Path
from subprocess import Popen, DEVNULL
from datetime import datetime, time, timedelta
import atexit, functools, datetime, pymongo.errors as mongoerrs
from dataclasses import dataclass, asdict

@dataclass
class DateRange:
    freq_min: int
    start: datetime
    end: datetime

    def __post_init__(self):
        self.check_freq_min(self.freq_min)
        self.check_datetime(self.start)
        self.check_datetime(self.end)
        assert self.start < self.end, f"start must be before end {self.start, self.end}"

    def generate_intervals(self) -> Generator[datetime, None, None]:
        curr = self.start
        while curr <= self.end:
            yield curr
            curr += timedelta(minutes=self.freq_min)

    @staticmethod
    def check_datetime(dt: datetime): assert dt.second == 0 and dt.microsecond == 0, f"datetime must have 0 second and microsend - {dt}"

    @staticmethod
    def check_freq_min(freq_min:int): assert freq_min in (1, 5, 15, 30, 60), f"{freq_min} is not a valid minute interval. Valid are 1, 5, 15, 30, 60."

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
    tz: ZoneInfo = ZoneInfo('UTC')

    intervals_schema: Dict = {
        'title': 'A sequence of datetimes',
        'required': ['datetime'],
        'properties': {'datetime': {'bsonType': 'date'},
        }
    }

    # tickers_schema and Tickers dataclass need to exacly agree in field name

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

    def __new__(cls, connstr:Optional[str]='localhost:27017', dbdir:Optional[Path | str]=None):
        if connstr is not None: assert dbdir is None, 'cannot specify a connection string and to start a local database'
        if dbdir is not None:
            try:
                cls.conndb()
                assert False, 'Local database already started(1)'
            except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure): pass
        return super().__new__(cls)

    def __init__(self, connstr:Optional[str]='localhost:27017', dbdir:Optional[Path | str]=None):
        # argument processing
        if dbdir is not None and isinstance(dbdir, str): dbdir = Path(dbdir)
        if dbdir is None and connstr is None: dbdir = Path('yaatdb_local')

        # cache arguments
        self.dbdir, self.connstr = dbdir, connstr

        # connect db
        self.dbc, self.mongo_proc = self.startlocdb(self.dbdir) if self.connstr is None else (self.conndb(self.connstr), None)

        # get the database from the client
        self.db = self.dbc[self.db_name]

        # create the tickers collection
        if 'tickers' in self.db.list_collection_names(): self.tickers_coll = self.db['tickers']
        else: self.tickers_coll = self.db.create_collection('tickers', validator={'$jsonSchema': self.tickers_schema})
        self.tickers_coll.create_index({'symbol':1, 'datetime':1}, unique=True)
        self.tickers_coll.create_index({'symbol':1})
        self.tickers_coll.create_index({'datetime':1})

        # create the intervals collection
        if 'intervals' in self.db.list_collection_names(): self.intervals_coll = self.db['intervals']
        else: self.intervals_coll = self.db.create_collection('intervals', validator={'$jsonSchema': self.intervals_schema})
        self.intervals_coll.create_index({'datetime':1}, unique=True)
    
    def insert_ticker(self, ticker:Ticker): self.tickers_coll.insert_one(asdict(ticker))

    def get_tickers(self, dr:DateRange, syms:List[str], dtonly:bool=True) -> List[SymDate | Ticker]:

        if dtonly: proj, rettype = {'datetime':1, 'symbol': 1, '_id':0}, SymDate
        else: proj, rettype = {'_id':0}, Ticker

        return list(map(lambda t: rettype(**t), self.tickers_coll.aggregate([
            {'$match': {
                'datetime': {'$gte': dr.start, '$lte': dr.end},
                'symbol': {'$in': syms},
                '$expr': {'$and': [
                    {'$eq': [{'$second': '$datetime'}, 0]},
                    {'$in': [{'$minute': '$datetime'}, list(range(0, 60, dr.freq_min))]}
                ]}
            }},
            {'$project': proj}
        ])))

    def fill_intervals_coll(self, dr: DateRange):
        for dt in dr.generate_intervals():
            doc = {'datetime': dt}
            self.intervals_coll.update_one(doc, {'$set': doc}, upsert=True)

    @classmethod
    def is_business_hours(cls, dt: datetime) -> bool:
        if dt.tzinfo is None: dt = dt.replace(tzinfo=cls.tz)
        dt = dt.astimezone(ZoneInfo('US/Eastern'))
        return dt.weekday() < 5 and time(9, 30) <= dt.time() <= time(16, 0)

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