from yaat.informer import InformerArgs, Informer
from yaat.util import killproc
from typing import Dict, Optional, Set, List, Tuple
from dataclasses import fields, field
from pathlib import Path
from pymongo import MongoClient
from subprocess import Popen, DEVNULL
from bson.timestamp import Timestamp
from dataclasses import asdict
from functools import reduce
from polygon import RESTClient
from datetime import datetime, timedelta
from bson import Int64, ObjectId
from dataclasses import dataclass
from pymongo.collection import Collection
import atexit, functools, gridfs, tempfile, numpy as np, pymongo.errors as mongoerrs, pandas as pd

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

class Maester:

    def_dbdir = Path('yaatdb_local')
    db_name: str = 'yaatdb'

    # schemas

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

    spy_1min_ohclv_schema = {
        'title': 'SPY candles every 1 minute from alphavantage',
        'required': ['open', 'high', 'low', 'close', 'volume', 'date'],
        'properties': {
            'open': {'bsonType': 'double'},
            'high': {'bsonType': 'double'},
            'low': {'bsonType': 'double'},
            'close': {'bsonType': 'double'},
            'volume': {'bsonType': 'int'},
            'date': {'bsonType': 'date'},
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

        # create schemas

        def create_collection(name, schema) -> Collection:
            if name in self.db.list_collection_names():
                self.db.command('collMod', name, validator={'$jsonSchema': schema})
            else:
                self.db.create_collection(name, validator={'$jsonSchema': schema})
            return self.db[name]


        self.informers = create_collection('informers', self.informers_schema)
        self.candles1min = create_collection('candles1min', self.candles1min_schema)
        self.predictions = create_collection('predictions', self.predictions_schema)
        self.spy_1min_ohclv = create_collection('spy_1min_ohclv', self.spy_1min_ohclv_schema)

        # create indexes

        self.candles1min.create_index(idx:={'ticker':1, 'date':1}, unique=True)
        self.candles1min.create_index(idx:={'ticker':1})
        self.candles1min.create_index(idx:={'date':1})

        self.informers.create_index(idx:={'name':1}, unique=True)

        self.predictions.create_index(idx:={'name':1}, unique=True)

        # TODO
        # self.spy_1min_ohclv.create_index(idx:={'date':1}, unique=True)

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

    # database operations

    def insert_informer(self, name:str, tickers:List[str], informer: Informer, fields: Set[str], alpha_dataset:bool=False):
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
            | {'num_params': Int64(informer.num_params)}
            | {'fields': list(fields)}
            | {'alpha_dataset': alpha_dataset})

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
        # get the dataframes
        dfs = {tick: pd.DataFrame(list(self.candles1min.find({'ticker': tick}, {'_id': 0}).sort('date', 1))) for tick in tickers}

        # process the columns
        for tick, df in dfs.items():
            # drop ticker column - NOTE: for now
            df.drop('ticker', axis=1, inplace=True)

            # prepend ticker name to field
            df.rename(columns={col: f"{tick}_{col}" for col in df.columns if col != 'date'}, inplace=True)

            # drop fields not specified
            if fields is not None:
                df.drop([col for col in df.columns if not any(field in col for field in fields) and col != 'date'], axis=1, inplace=True)

        # merge the ticker dataframes
        result_df = reduce(lambda left, right: pd.merge(left, right, on='date', how='outer'), dfs.values())

        # clean nulls
        result_df.dropna(inplace=True)

        # get date ranges
        if start_date is not None:
            result_df = result_df[result_df['date'] >= start_date]
        if end_date is not None:
            result_df = result_df[result_df['date'] <= end_date]

        # save to file
        temp_file_path = tempfile.NamedTemporaryFile(delete=False).name + '.csv'
        result_df.to_csv(temp_file_path)

        # return size, filepath, and fields
        return result_df


    # def get_dataset(self, tickers:List[str], fields:Optional[List[str]]=None, max:Optional[int]=None, alpha_dataset:bool=False) -> Tuple[int, Path, Set[str]]:
    #     # prepend column names with ticker and drop the ticker column
    #     if not alpha_dataset:
    #         dfs = {tick: pd.DataFrame(list(self.candles1min.find({'ticker': tick}, {'_id': 0}).sort('date', 1))) for tick in tickers}
    #     else:
    #         dfs = {'SPY': pd.DataFrame(list(self.spy_1min_ohclv.find({}, {'_id': 0}).sort('date', 1)))}
    #     for tick, df in dfs.items():
    #         if not alpha_dataset:
    #             df.rename(columns={'volume': f'{tick}_volume', 'open': f'{tick}_open', 'close': f'{tick}_close',
    #                             'high': f'{tick}_high', 'low': f'{tick}_low', 'transactions': f'{tick}_transactions'},
    #                             inplace=True)
    #         else:
    #             df.rename(columns={'volume': f'{tick}_volume', 'open': f'{tick}_open', 'close': f'{tick}_close',
    #                             'high': f'{tick}_high', 'low': f'{tick}_low'},
    #                             inplace=True)


    #         # the ticker in the column name now
    #         if not alpha_dataset: df.drop('ticker', axis=1, inplace=True)

    #         # drop more fields
    #         if fields is not None:
    #             df.drop([col for col in df.columns if not any(field in col for field in fields) and col != 'date'], axis=1, inplace=True)

    #     # merge the ticker dataframes
    #     result_df = reduce(lambda left, right: pd.merge(left, right, on='date', how='outer'), dfs.values())

    #     # clean nulls
    #     result_df.dropna(inplace=True)

    #     # enforce max
    #     if max is not None:
    #         result_df = result_df.tail(max)

    #     # save to file
    #     temp_file_path = tempfile.NamedTemporaryFile(delete=False).name + '.csv'
    #     result_df.to_csv(temp_file_path)

    #     # get the fields
    #     fields = set(map(lambda x: x.split('_')[1], set(result_df.columns) - {'date'}))

    #     # return size, filepath, and fields
    #     return len(result_df), Path(temp_file_path), fields

    def get_prediction_data(self, start_date: str, informer_doc: Dict) -> Tuple[str, Path]:
        # create the ticker dataframes
        curr_date = datetime.strptime(start_date, '%Y-%m-%d')
        dfs = {}
        for tick in informer_doc['tickers']:
            # grab the data
            df = pd.DataFrame(map(asdict, self.polygon.get_aggs(tick, 1, 'minute', curr_date.strftime('%Y-%m-%d'), curr_date.strftime('%Y-%m-%d'), adjusted=True)))
            while (len(df)) < informer_doc['seq_len']*2: # *2 incase later processing trims it down
                curr_date = curr_date - timedelta(days=1)
                new_df = pd.DataFrame(map(asdict, self.polygon.get_aggs(tick, 1, 'minute', curr_date.strftime('%Y-%m-%d'), curr_date.strftime('%Y-%m-%d'), adjusted=True)))
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

        # drop columns not in fields
        result_df.drop([col for col in result_df.columns if not any(field in col for field in informer_doc['fields']) and col != 'date'], axis=1, inplace=True)

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
    

# TODO - for array element types
# book_validator = {
#     "$jsonSchema": {
#         "bsonType": "object",
#         "required": ["authors"],  # Example of a required field
#         "properties": {
#             "authors": {
#                 "bsonType": "array",
#                 "description": "must be an array of strings and is required",
#                 "items": {
#                     "bsonType": "string",
#                     "description": "each item must be a string"
#                 }
#             }
#         }
#     }
# }