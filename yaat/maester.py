from __future__ import annotations
from yaat.util import killproc
from typing import Optional, Tuple, Dict, List, Generator
from pymongo import MongoClient, UpdateOne
from zoneinfo import ZoneInfo
from pathlib import Path
from subprocess import Popen, DEVNULL
from datetime import datetime, time, timedelta
from dataclasses import dataclass, asdict
import atexit, functools, datetime, pymongo.errors as mongoerrs, pandas as pd

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
    tz: ZoneInfo = ZoneInfo('UTC')
    binsearch_threshold = 10000

    intervaltimes_schema: Dict = {
        'title': 'A sequence of datetimes',
        'required': ['datetime'],
        'properties': {'datetime': {'bsonType': 'date'},
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
        self.intervaltimes = create_collection('intervaltimes', self.intervaltimes_schema)
        self.intervaltimes.create_index({'datetime':1}, unique=True)
    
    def insert_ticker(self, ticker:Ticker): self.tickers.insert_one(asdict(ticker))

    def insert_intervaltime(self, dt:datetime):
        DateRange.check_datetime(dt)
        self.intervaltimes.insert_one({'datetime': dt})

    @staticmethod
    def get_dr_match_agg_stage(dr:DateRange) -> Dict:
        return {'$match': {
                'datetime': {'$gte': dr.start, '$lte': dr.end}},
                '$expr': {'$and': [
                    {'$eq': [{'$second': '$datetime'}, 0]},
                    {'$in': [{'$minute': '$datetime'}, list(range(0, 60, dr.freq_min))]}
                ]}
        }

    def get_intervals(self, dr:DateRange) -> pd.DatetimeIndex:
        cur = self.intervals_coll.aggregate([
            self.get_dr_match_agg_stage(dr),
            {'$project': {'datetime': 1, '_id': 0}},
        ])
        return pd.DatetimeIndex([doc['datetime'] for doc in cur])

    def get_missing_intervals(self, dr:DateRange) -> pd.DatetimeIndex: return dr.intervals.difference(self.get_intervals(dr))

    def fill_intervals(self, dr:DateRange) -> int:
        missing = self.get_missing_intervals(dr)
        bops = [UpdateOne((doc:={'datetime': dt}), {'$set': doc}, upsert=True) for dt in missing]
        return self.intervals_coll.bulk_write(bops).inserted_count if bops else 0

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

    def _fill_intervals_coll(self, dr: DateRange) -> int: # simple, no binary search
        bops = [UpdateOne((doc:={'datetime': dt}), {'$set': doc}, upsert=True) for dt in dr.generate_intervals()]
        if bops: return self.intervals_coll.bulk_write(bops).inserted_count
        return 0

    def fill_intervals_coll(self, dr: DateRange) -> int: # binary search

        def count_docs(_dr: DateRange) -> int:
            return self.intervals_coll.count_documents({'datetime': {'$gte': _dr.start, '$lt':  _dr.end}})

        def round_datetime(dt):
            return (dt + timedelta(seconds=30)).replace(second=0, microsecond=0)

        inserted_count = 0
        dr2search = [dr]
        while len(dr2search) > 0:
            curr_dr = dr2search.pop()
            if curr_dr.num_intervals == count_docs(curr_dr): continue
            if curr_dr.num_intervals < self.binsearch_threshold:
                bops = [UpdateOne((doc:={'datetime': dt}), {'$set': doc}, upsert=True) for dt in dr.generate_intervals()]
                if bops: inserted_count += self.intervals_coll.bulk_write(bops).inserted_count
                continue
            mid_dt = round_datetime(curr_dr.start + (curr_dr.end - curr_dr.start) // 2)
            dr2search.append(DateRange(dr.freq_min, curr_dr.start, mid_dt)) # left
            dr2search.append(DateRange(dr.freq_min, mid_dt, curr_dr.end)) # right
        return inserted_count


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