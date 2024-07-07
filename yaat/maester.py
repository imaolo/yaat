from __future__ import annotations
from typing import Dict, Optional, TYPE_CHECKING
from dataclasses import fields
from pathlib import Path
from pymongo import MongoClient
from subprocess import Popen, DEVNULL
from yaat.informer import InformerArgs
from yaat.util import killproc
import atexit, functools, pymongo.errors as mongoerrs

if TYPE_CHECKING:
    from pymongo.collection import Collection

pybson_tmap = {
    str: 'string',
    int: 'int',
    bool: 'bool',
    float: 'double',
    Optional[str]: ['string', 'null']
}

class Maester:

    def_dbdir = Path('yaatdb_local')
    db_name: str = 'yaatdb'
    
    informer_weights_schema: Dict = {
        'title': 'Weights for informer models',
        'required': [field.name for field in fields(InformerArgs)]
                        + ['weights', 'dataset'],
        'properties': {field.name: {'bsonType': pybson_tmap[field.type]} for field in fields(InformerArgs)}
                        | {'weights': {'bsonType': 'binData'}}
                        | {'dataset': {'bsonType': 'object'}}
    }

    # construction

    def __init__(self, connstr:Optional[str]=None, dbdir:Optional[Path | str]=None):
        self.connstr = connstr
        self.dbdir = dbdir

        # database start or connect
        if self.connstr is None: # start
            try:
                self.conndb(timeout=1000)
                raise RuntimeError('Local database already started')
            except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure): pass
            self.mongo_proc = self.startlocdb(self.dbdir if self.dbdir is not None else self.def_dbdir)
        else: # connect (dbdir must be none)
            self.mongo_proc = None
            if self.dbdir is not None: raise RuntimeError('cannot specify a connection string and to start a local database')
        self.dbc = self.conndb(self.connstr)
        self.db = self.dbc[self.db_name]

        # database schemas

        def create_collection(name, schema) -> Collection:
            if name in self.db.list_collection_names(): return self.db[name]
            else: return self.db.create_collection(name, validator={'$jsonSchema': schema})

        self.informer_weights = create_collection('informer_weights', self.informer_weights_schema)

    # database

    @classmethod
    def conndb(cls, url:Optional[str]=None, timeout:int=5000) -> MongoClient:
        (c := MongoClient(url, serverSelectionTimeoutMS=timeout))['admin'].command('ping')
        return c

    @classmethod
    def startlocdb(cls, dbdir:Path) -> Popen:
        dbdir.mkdir(parents=True, exist_ok=True)
        try: 
            cls.conndb()
            assert False, 'Local database already started(2)'
        except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure):
            try: mongo_proc = Popen(['mongod', '--dbpath', str(dbdir.absolute())], stdout=DEVNULL, stderr=DEVNULL) # NOTE: --logpath
            except Exception as e: print(f"mongod failed: {e}"); raise
            atexit.register(functools.partial(killproc, mongo_proc))
            return mongo_proc

    def __del__(self):
        if hasattr(self, 'mongo_proc') and self.mongo_proc is not None and killproc is not None:
            killproc(self.mongo_proc)