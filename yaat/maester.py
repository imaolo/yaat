from yaat.informer import InformerArgs, Informer
from yaat.util import killproc
from typing import Dict, Optional, List
from dataclasses import fields, field
from pathlib import Path
from pymongo import MongoClient
from subprocess import Popen, DEVNULL
from functools import reduce
from datetime import datetime
from bson import Int64, ObjectId
from dataclasses import dataclass, asdict
from pymongo.collection import Collection
import atexit, functools, gridfs, polygon, numpy as np, pymongo.errors as mongoerrs, pandas as pd

pybson_tmap = {
    str: {'bsonType': 'string'},
    int: {'bsonType': 'int'},
    bool: {'bsonType': 'bool'},
    float: {'bsonType': 'double'},
    Optional[str]: {'bsonType': ['string', 'null']},
    Optional[np.array]: {'bsonType': ['array', 'null']},
    Optional[ObjectId]: {'bsonType': ['null', 'objectId']},
    List[str]: {'bsonType': 'array', 'items': {'bsonType': 'string'}},
    List[float]: {'bsonType': 'array', 'items': {'bsonType': 'double'}},
    datetime: {'bsonType': 'date'},
    Optional[Int64]: {'bsonType': ['null', 'long']},
    Optional[float]: {'bsonType':['double', 'null']},
    Optional[int]: {'bsonType':['int', 'null']},
}

@dataclass(kw_only=True)
class InformerDoc(InformerArgs):
    tickers: List[str]
    name: str
    fields: List[str]
    date: datetime = field(default_factory=datetime.now)
    alpha_dataset: bool = False
    num_params: Optional[Int64] = None
    weights_file_id: Optional[ObjectId] = None
    curr_epoch: Optional[int] = None
    train_loss: Optional[float] = None
    vali_loss: Optional[float] = None
    test_loss: Optional[float] = None
    left_time: Optional[float] = None

@dataclass
class PredictionDoc:
    name:str
    model_name:str
    pred_date:datetime
    predictions:List[float]
    date: datetime = field(default_factory=datetime.now)

@dataclass
class CandleDoc:
    open:float
    close:float
    high:float
    low:float
    volume:float

class Maester:

    def_dbdir = Path('yaatdb_local')
    db_name: str = 'yaatdb'

    # api info

    alpha_url = 'https://www.alphavantage.co/query?'
    alpha_key = '3KZFIF8WVK43Q92B'

    polygon_key = 'fuqZHZzJdzJpYq2kMRxZTI42N1nPlxKj'

    # schemas

    candle_schema = {
        'title': 'A collection to hold candles for a single ticker',
        'required': [field.name for field in fields(CandleDoc)],
        'properties': {field.name: pybson_tmap[field.type] for field in fields(CandleDoc)}
    } 

    informers_schema: Dict = {
        'title': 'Weights for informer models',
        'required': [field.name for field in fields(InformerDoc)],
        'properties': {field.name: pybson_tmap[field.type] for field in fields(InformerDoc)}
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
        'required': [field.name for field in fields(PredictionDoc)],
        'properties': {field.name: pybson_tmap[field.type] for field in fields(PredictionDoc)}
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

        # create collections

        self.informers = self.init_collection('informers', self.informers_schema)
        self.candles1min = self.init_collection('candles1min', self.candles1min_schema)
        self.predictions = self.init_collection('predictions', self.predictions_schema)

        # create indexes

        self.candles1min.create_index({'ticker':1, 'date':1}, unique=True)
        self.candles1min.create_index({'ticker':1})
        self.candles1min.create_index({'date':1})

        self.informers.create_index({'name':1}, unique=True)

        self.predictions.create_index({'name':1}, unique=True)

        # file store

        self.fs = gridfs.GridFS(self.db)

        # data api

        self.polygon = polygon.RESTClient(api_key=self.polygon_key)

    # database config

    def init_collection(self, name, schema) -> Collection:
        if name in self.db.list_collection_names():
            self.db.command('collMod', name, validator={'$jsonSchema': schema})
        else:
            self.db.create_collection(name, validator={'$jsonSchema': schema})
        return self.db[name]

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

    # database operations

    def set_informer_weights(self, name: str, informer:Informer) -> ObjectId:
        # delete the old file
        model_doc = self.informers.find_one(q:={'name': name})
        if model_doc['weights_file_id'] is not None: self.fs.delete(model_doc['weights_file_id'])
         
        # upload weights file
        weights_file_id = self.fs.put(informer.byte_weights)

        # set the new weights file id
        self.informers.update_one(q, {'$set': {'weights_file_id': weights_file_id}})

        return weights_file_id

    def get_dataset(self, tickers:List[str], fields:Optional[List[str]]=None,
                    start_date:Optional[datetime]=None, end_date:Optional[datetime]=None) -> pd.DataFrame:
        # projection and sort stage
        proj_sort_stage =  [{'$project': {'_id': 0, **({field:1 for field in fields + ['date']} if fields is not None else {'ticker': 0})}},
                            {'$sort': {'date': 1}}]

        # date stage
        date_conditions = {}
        if start_date: date_conditions['$gte'] = start_date
        if end_date: date_conditions['$lte'] = end_date
        date_stage = [{'$match': {'date': date_conditions}}] if date_conditions else []

        # per ticker factory
        tick_stage_factory = lambda tick: [{'$match': {'ticker': tick}}]

        # query and get dataframes
        dfs = {tick: pd.DataFrame(list(self.candles1min.aggregate(tick_stage_factory(tick) + date_stage + proj_sort_stage))) for tick in tickers}

        # prepend ticker name to column names
        for tick, df in dfs.items():
            df.rename(columns={col: f"{tick}_{col}" for col in df.columns if col != 'date'}, inplace=True)

        # merge the ticker dataframes
        result_df = reduce(lambda left, right: pd.merge(left, right, on='date', how='outer'), dfs.values())

        # clean nulls
        result_df.dropna(inplace=True)

        return result_df

    def get_live_data(self, tickers:List[str], fields:List[str], start_date:datetime, end_date:datetime, timespan:str='minute'):
        # get a dictionary of dataframes
        dfs = {}
        for tick in tickers:
            # grab the ticker data as a dataframe
            df = pd.DataFrame(map(asdict, self.polygon.get_aggs(tick, 1, timespan, start_date, end_date, adjusted=True)))
            
            # drop unneeded
            df.drop(['vwap', 'otc'], axis=1, inplace=True)

            # rename the non-dates
            df.rename(columns={col:f"{tick}_{col}" for col in df.columns if col!='timestamp'}, inplace=True)

            # store df in dictionary
            dfs[tick] = df

        # join
        df = reduce(lambda left, right: pd.merge(left, right, on='timestamp', how='outer'), dfs.values())

        # sort
        df.sort_values(by='timestamp', ascending=True, inplace=True)

        # convert timestamp to date
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df.drop('timestamp', axis=1, inplace=True)

        # drop columns not in fields
        df.drop([col for col in df.columns if not any(field in col for field in fields) and col != 'date'], axis=1, inplace=True)

        # clean nulls
        df.dropna(inplace=True)

        return df

    # tickers db

    def create_tickers_dataset(self, ticker:str):
        assert ticker not in self.db.list_collection_names(), self.db.list_collection_names()

        # create the collection
        tick_coll = self.init_collection(ticker, self.candle_schema)

        # no duplicate dates
        tick_coll.create_index({'date': 1}, unique=True)

        # TODO - mine that shit