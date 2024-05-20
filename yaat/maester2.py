from __future__ import annotations
from yaat.util import mkdirs, killproc
from typing import Optional
from pymongo import MongoClient
from zoneinfo import ZoneInfo
import subprocess, atexit, functools, pymongo.errors as mongoerrs

class _Maester:
    db_name: str = 'yaatdb'
    tz: ZoneInfo = ZoneInfo('UTC') 

    def __init__(self, connstr:Optional[str]='mongodb://54.205.245.140:27017/'):
        # create (and possibly start) db
        self.connstr = connstr
        self.dbc = self.startlocdb() if self.connstr is None else self.conndb(self.connstr)
        self.db = self.dbc[self.db_name]

        # create the tickers collection
        self.tickers_schema = {
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
        if 'tickers' in self.db.list_collection_names(): self.tickers_coll = self.db['tickers']
        else: self.tickers_coll = self.db.create_collection('tickers', validator={'$jsonSchema': self.tickers_schema })
        self.tickers_coll.create_index({'symbol':1, 'datetime':1}, unique=True)
        self.tickers_coll.create_index({'symbol':1})
        self.tickers_coll.create_index({'datetime':1})
    
    @classmethod
    def conndb(cls, url:Optional[str]=None) -> MongoClient:
        (c := MongoClient(url, serverSelectionTimeoutMS=5000))['admin'].command('ping')
        return c
    
    @classmethod
    def startlocdb(cls, dir:str='./yaatdb_local') -> MongoClient:
        mkdirs(dir, exist_ok=True)
        try: return cls.conndb()
        except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure):
            try: mongod = subprocess.Popen(['mongod', '--dbpath', dir], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) # NOTE: --logpath
            except Exception as e: print(f"mongod failed: {e}"); raise
            atexit.register(functools.partial(killproc, mongod, 'mongod'))
            return cls.conndb()
Maester = _Maester()