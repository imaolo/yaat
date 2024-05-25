from __future__ import annotations
from yaat.util import killproc
from typing import Optional, Tuple, Dict, Any
from pymongo import MongoClient
from zoneinfo import ZoneInfo
from pathlib import Path
from subprocess import Popen, DEVNULL
import atexit, functools, datetime, pymongo.errors as mongoerrs
from dataclasses import dataclass

class Maester:
    db_name: str = 'yaatdb'
    tz: ZoneInfo = ZoneInfo('UTC')

    # tickers_schema and tickers dataclass need to exacly agree in field name

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

    @dataclass  
    class tickers_class:
        symbol: str
        datetime: datetime.datetime
        open: float
        close: float
        high: float
        low: float
        volume: Optional[int] = None

    def __new__(cls, connstr:Optional[str]='mongodb://54.205.245.140:27017/', dbdir:Optional[Path | str]=None):
        if connstr is not None: assert dbdir is None, 'cannot specify a connection string and to start a local database'
        if dbdir is not None:
            try:
                cls.conndb()
                assert False, 'Local database already started(1)'
            except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure): pass
        return super().__new__(cls)

    def __init__(self, connstr:Optional[str]='mongodb://54.205.245.140:27017/', dbdir:Optional[Path | str]=None):
        # argument processing
        if dbdir is not None and isinstance(dbdir, str): dbdir = Path(dbdir)
        if dbdir is None and connstr is None: self.dbdir = Path('yaatdb_local')

        # cache arguments
        self.dbdir = dbdir
        self.connstr = connstr

        # connect db
        if self.connstr is None: self.dbc, self.mongo_proc = self.startlocdb(self.dbdir)
        else: self.dbc = self.conndb(self.connstr)

        # get the database from the client
        self.db = self.dbc[self.db_name]

        # create the tickers collection (schema and indexes too)
        if 'tickers' in self.db.list_collection_names(): self.tickers_coll = self.db['tickers']
        else: self.tickers_coll = self.db.create_collection('tickers', validator={'$jsonSchema': self.tickers_schema})
        self.tickers_coll.create_index({'symbol':1, 'datetime':1}, unique=True)
        self.tickers_coll.create_index({'symbol':1})
        self.tickers_coll.create_index({'datetime':1})
    
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