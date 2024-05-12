from yaat.util import mkdirs, killproc
from typing import Optional, TYPE_CHECKING
from pymongo import MongoClient
import subprocess, atexit, functools, pymongo.errors as mongoerrs

class Maester:
    db_name: str = 'yaatdb'

    def __init__(self, connstr:Optional[str]='mongodb://54.205.245.140:27017/'):
        self.connstr = connstr
        self.dbc = self.startlocdb() if self.connstr is None else self.conndb(self.connstr)
        self.db = self.dbc[self.db_name]

        '''
            define the database schema

            collections:

            - weights (status name, dataset, weights, loss)
            - datasets (status, collection name, info, panda bytes)
            - predictions (status, dataset, model, prediction data)

            - data collections (dynamically named, dynamic schema)
        '''

        weights_schema = {
            'title': 'A collection of model weights',
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
                    'bsonType': 'double',
                    'description': 'the latest training loss'
                },
                'train_loss': {
                    'bsonType': 'double',
                    'description': 'the latest validation loss'
                },
                'status': {
                    'bsonType': 'string',
                    'description': 'the status of the model (training, error, completed, etc)'
                }
            }
        }

        if 'weights' not in self.db.list_collection_names():
            self.db.create_collection('weights', validator={'$jsonSchema': weights_schema})
    
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


