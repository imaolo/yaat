from __future__ import annotations
from yaat.util import mkdirs, killproc
from typing import TYPE_CHECKING, Optional, Dict
from pymongo import MongoClient
import subprocess, atexit, functools, pymongo.errors as mongoerrs

if TYPE_CHECKING:
    import io
    from pymongo.database import Collection

class Maester:
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

        # create the weights collection
        self.weights_schema = {
            'title': 'model weights',
            'required': ['name', 'dataset', 'weights', 'train_loss', 'val_loss', 'status'],
            'properties': {
                'name': {
                    'bsonType': 'string',
                    'description': 'name assigned to the weights'
                },
                'dataset':{
                    'bsonType': 'objectId',
                    'description': 'dataset document within the datasets collection'
                },
                'weights': {
                    'bsonType': 'binData',
                    'description': 'pytorch model'
                },
                'train_loss': {
                    'bsonType': ['double', 'null'],
                    'description': 'the latest training loss'
                },
                'train_loss': {
                    'bsonType': ['double', 'null'],
                    'description': 'the latest validation loss'
                },
                'status': {
                    'bsonType': 'string',
                    'description': 'the status of the model (training, error, completed, etc)'
                }
            }
        }
        self.weights_coll = create_get_coll('weights', self.weights_schema)

        # create the datasets metadata collection
        self.datasets_schema = {
            'title': 'metadata used to describe and locate datasets',
            'required': ['name', 'status', 'data'],
            'properties': {
                'name': {
                    'bsonType': 'string',
                    'description': "the dataset's name and the name of the collection where the data in document form lives"
                },
                'status': {
                    'bsonType': 'string',
                    'description': 'the status of the dataset (started, collecting, collected, finished, etc)'
                },
                'data': {
                    'bsonType': ['binData', 'null'],
                    'description': 'the dataframe form of the dataset'
                }
            }
        }
        self.datasets_coll = create_get_coll('datasets', self.datasets_schema)

        # TODO - predictions collection

    def create_weights(self, name:str, dataset:str, weights: io.BytesIO):
        ds_docs = list(self.datasets_coll.find({'name': dataset}))
        assert len(ds_docs) == 1
        self.weights_coll.insert_one({
            'name': name,
            'dataset': ds_docs[0]['_id'],
            'weights': weights,
            'train_loss': None,
            'val_loss': None,
            'status': 'created'
        })

    def create_dataset(self, name:str, schema:Optional[Dict]=None): # TODO - dataset schema
        assert name not in self.db.list_collection_names()
        self.db.create_collection(name, validator={'$jsonSchema': schema} if schema is not None else None)
        self.datasets_coll.insert_one({
            'name': name,
            'status': 'created',
            'data': None
        })
    
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