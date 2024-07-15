from __future__ import annotations
from typing import Dict, Optional, Set, List, Tuple, TYPE_CHECKING
from dataclasses import fields
from pathlib import Path
from pymongo import MongoClient
from subprocess import Popen, DEVNULL
from yaat.informer import InformerArgs
from yaat.util import killproc
from bson.timestamp import Timestamp
from dataclasses import asdict
from functools import reduce
from polygon import RESTClient
from pprint import pprint
from datetime import datetime, timedelta
import atexit, functools, gridfs, io, torch, tempfile, numpy as np, pymongo.errors as mongoerrs, pandas as pd

if TYPE_CHECKING:
    from pymongo.collection import Collection
    from yaat.informer import Informer

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
                        + ['weights_file_id', 'tickers', 'settings', 'timestamp', 'name', 'num_params', 'curr_epoch', 'train_loss', 'vali_loss', 'test_loss', 'left_time'],
        'properties': {field.name: {'bsonType': pybson_tmap[field.type]} for field in fields(InformerArgs)}
                        | {'weights_file_id': {'bsonType': ['null', 'objectId']}}
                        | {'tickers': {'bsonType': 'array'}}
                        | {'settings': {'bsonType': 'string'}}
                        | {'timestamp': {'bsonType': 'timestamp'}}
                        | {'name': {'bsonType': 'string'}}
                        | {'num_params': {'bsonType': 'int'}}
                        | {'curr_epoch': {'bsonType': 'int'}}
                        | {'train_loss': {'bsonType': ['double', 'null']}}
                        | {'vali_loss': {'bsonType': ['double', 'null']}}
                        | {'test_loss': {'bsonType': ['double', 'null']}}
                        | {'left_time': {'bsonType': ['double', 'null']}}
    }

    candles1min_schema = {
        'title': 'Candles every 1 minute',
        'required': ['ticker', 'volume', 'open', 'close', 'high', 'low', 'date', 'transactions'],
        'properties': {
            'ticker': {'bsonType': 'string'},
            'volume': {'bsonType': 'long'},
            'open': {'bsonType': 'double'},
            'close': {'bsonType': 'double'},
            'high': {'bsonType': 'double'},
            'low': {'bsonType': 'double'},
            'date': {'bsonType': 'date'},
            'transactions': {'bsonType': 'long'},
        }
    }

    predictions_schema = {
        'title': 'predictions',
        'required': ['name', 'model_name', 'last_date', 'predictions', 'timestamp'],
        'properties': {
            'name': {'bsonType': 'string'},
            'model_name': {'bsonType': 'string'},
            'last_date': {'bsonType': 'date'},
            'predictions': {'bsonType': 'array'},
            'timestamp': {'bsonType': 'date'},

        }
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

        # create schemas

        def create_collection(name, schema) -> Collection:
            if name in self.db.list_collection_names():
                self.db.command('collMod', name, validator={'$jsonSchema': schema})
            else:
                self.db.create_collection(name, validator={'$jsonSchema': schema})
            return self.db[name]


        self.informer_weights = create_collection('informer_weights', self.informer_weights_schema)
        self.candles1min = create_collection('candles1min', self.candles1min_schema)
        self.predictions = create_collection('predictions', self.predictions_schema)

        # create indexes

        self.candles1min.create_index(idx:={'ticker':1, 'date':1}, unique=True)
        self.candles1min.create_index(idx:={'ticker':1})
        self.candles1min.create_index(idx:={'date':1})

        self.informer_weights.create_index(idx:={'name':1}, unique=True)

        self.predictions.create_index(idx:={'name':1}, unique=True)

        # file store

        self.fs = gridfs.GridFS(self.db)

        # polygon - fuqZHZzJdzJpYq2kMRxZTI42N1nPlxKj

        self.polygon = RESTClient(api_key='fuqZHZzJdzJpYq2kMRxZTI42N1nPlxKj')

    # database config

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

    # database ops

    def insert_informer(self, name:str, tickers:List[str], informer: Informer):
        self.informer_weights.insert_one(asdict(informer.og_args)
            | {'settings' : informer.settings}
            | {'timestamp': Timestamp(int(informer.timestamp), 1)}
            | {'weights_file_id': None}
            | {'tickers': tickers}
            | {'train_loss': None}
            | {'vali_loss': None}
            | {'test_loss': None}
            | {'curr_epoch': 0}
            | {'left_time': None}
            | {'name': name}
            | {'num_params': informer.num_params})

    def set_informer_weights(self, informer:Informer):
        # TODO - should delete the old file
         
        # upload weights file
        weights_file_id = self.fs.put(informer.byte_weights)

        # set the new weights file id
        self.informer_weights.update_one({'settings': informer.settings, 'timestamp': Timestamp(int(informer.timestamp), 1)},
                                         {'$set': {'weights_file_id': weights_file_id}})

    # database properties

    @property
    def data_collections(self) -> Set[str]:
        return set(self.db.list_collection_names()) - set(['informer_weights', 'fs.chunks', 'fs.files'])

    # misc

    def get_dataset(self, tickers:List[str]) -> Tuple[int, Path]:
        # prepend column names with ticker and drop the ticker column
        dfs = {tick: pd.DataFrame(list(self.candles1min.find({'ticker': tick}, {'_id': 0}).sort('date', 1))) for tick in tickers}
        for tick, df in dfs.items():
            df.rename(columns={'volume': f'{tick}_volume', 'open': f'{tick}_open', 'close': f'{tick}_close',
                               'high': f'{tick}_high', 'low': f'{tick}_low', 'transactions': f'{tick}_transactions'},
                               inplace=True)
            df.drop('ticker', axis=1, inplace=True)

        # merge the ticker dataframes
        result_df = reduce(lambda left, right: pd.merge(left, right, on='date', how='outer'), dfs.values())

        # clean nulls
        result_df.dropna(inplace=True)

        # save to file
        temp_file_path = tempfile.NamedTemporaryFile(delete=False).name + '.csv'
        result_df.to_csv(temp_file_path)

        # return size and filepath
        return len(result_df), Path(temp_file_path)

    def get_prediction_data(self, start_date: str, informer_doc: Dict) -> Tuple[str, Path]:
        # create the ticker dataframes
        curr_date = datetime.strptime(start_date, '%Y-%m-%d')
        dfs = {}
        for tick in informer_doc['tickers']:
            # grab the data
            df = pd.DataFrame(map(asdict, self.polygon.get_aggs('AAPL', 1, 'minute', curr_date.strftime('%Y-%m-%d'), curr_date.strftime('%Y-%m-%d'), adjusted=True)))
            while (len(df)) < informer_doc['seq_len']*2: # *2 incase later processing trims it down
                curr_date = curr_date - timedelta(days=1)
                new_df = pd.DataFrame(map(asdict, self.polygon.get_aggs('AAPL', 1, 'minute', curr_date.strftime('%Y-%m-%d'), curr_date.strftime('%Y-%m-%d'), adjusted=True)))
                df = pd.concat([df, new_df])

            # drop unneeded
            df.drop(['vwap', 'otc'], axis=1, inplace=True)

            # rename the non-dates
            df.rename(columns={'volume': f'{tick}_volume', 'open': f'{tick}_open', 'close': f'{tick}_close',
                               'high': f'{tick}_high', 'low': f'{tick}_low', 'transactions': f'{tick}_transactions'},
                               inplace=True)

            # store df in dictionary
            dfs[tick] = df

        # join
        result_df = reduce(lambda left, right: pd.merge(left, right, on='timestamp', how='outer'), dfs.values())

        # sort
        result_df.sort_values(by='timestamp', ascending=True, inplace=True)

        # convert timestamp to date
        result_df['timestamp'] = pd.to_datetime(result_df['timestamp'], unit='ms')
        result_df['date'] = result_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        result_df.drop('timestamp', axis=1, inplace=True)

        # clean nulls
        result_df.dropna(inplace=True)

        # save to file
        temp_file_path = tempfile.NamedTemporaryFile(delete=False).name + '.csv'
        result_df.to_csv(temp_file_path)

        # return size and filepath
        return result_df.tail(1)['date'].values[0], Path(temp_file_path)
    

    def store_predictions(self, name:str, model_name:str, pred_date:str, pred_fp:Path) -> datetime:

        preds = np.load(pred_fp)
        timestamp = datetime.now()

        self.predictions.insert_one({
            'name': name,
            'model_name': model_name,
            'last_date': datetime.strptime(pred_date, '%Y-%m-%d %H:%M:%S'),
            'predictions': preds.tolist(),
            'timestamp': timestamp
        })

        return timestamp


        
