from __future__ import annotations
from yaat.util import mkdirs, killproc
from typing import Optional, TYPE_CHECKING
from pymongo import MongoClient
import subprocess, atexit, functools, pymongo.errors as mongoerrs
from bson import ObjectId

if TYPE_CHECKING:
    import io

class Maester:
    db_name: str = 'yaatdb'

    def __init__(self, connstr:Optional[str]='mongodb://54.205.245.140:27017/'):
        self.connstr = connstr
        self.dbc = self.startlocdb() if self.connstr is None else self.conndb(self.connstr)
        self.db = self.dbc[self.db_name]

        # predictions (status, dataset, model, prediction data)
        # data collections (dynamically named, dynamic schema)

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
        if 'weights' not in self.db.list_collection_names():
            self.weights_coll = self.db.create_collection('weights', validator={'$jsonSchema': self.weights_schema})
            self.weights_coll.create_index({'name':1}, unique=True)
        else:
            self.weights_coll = self.db['weights']

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
        if 'datasets' not in self.db.list_collection_names():
            self.datasets_coll = self.db.create_collection('datasets', validator={'$jsonSchema': self.datasets_schema})
            self.datasets_coll.create_index({'name':1}, unique=True)
        else:
            self.datasets_coll = self.db['datasets']

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

    def create_dataset(self, name:str): # TODO - dataset schema
        assert name not in self.db.list_collection_names()
        self.db.create_collection(name)
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
        mkdirs(dir)
        try: return cls.conndb()
        except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure):
            try: mongod = subprocess.Popen(['mongod', '--dbpath', dir], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) # NOTE: --logpath
            except Exception as e: print(f"mongod failed: {e}"); raise
            atexit.register(functools.partial(killproc, mongod, 'mongod'))
            return cls.conndb()