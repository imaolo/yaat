from __future__ import annotations
from yaat.util import mkdirs, killproc
from typing import TYPE_CHECKING, Optional, Dict
from pymongo import MongoClient
import subprocess, atexit, functools, pymongo.errors as mongoerrs

if TYPE_CHECKING:
    import io
    from pymongo.database import Collection

class _Maester:
    db_name: str = 'yaatdb'

    def __init__(self, connstr:Optional[str]='mongodb://54.205.245.140:27017/'):
        # create (and possibly start) db
        self.connstr = connstr
        self.dbc = self.startlocdb() if self.connstr is None else self.conndb(self.connstr)
        self.db = self.dbc[self.db_name]

        # helper
        def create_get_coll(name:str, schema:Dict) -> Collection:
            if name in self.db.list_collection_names(): return self.db[name]
            coll = self.db.create_collection(name, validator={'$jsonSchema': schema})
            coll.create_index({'name':1}, unique=True)
            return coll

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
        self.tickers_coll = create_get_coll('tickers', self.tickers_schema)
    
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