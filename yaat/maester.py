from yaat.informer import InformerArgs, Informer
from yaat.util import killproc, fetchjson
from typing import Dict, Optional, List
from dataclasses import fields, field
from pathlib import Path
from pymongo import MongoClient
from subprocess import Popen, DEVNULL
from functools import reduce
from datetime import datetime, timedelta
from bson import Int64, ObjectId
from dataclasses import dataclass, asdict
from pymongo.collection import Collection
import atexit, functools, gridfs, tqdm, polygon, time, numpy as np, pymongo.errors as mongoerrs, pandas as pd

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
    Optional[datetime]: {'bsonType': ['null', 'date']},
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
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

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
    volume:int

class Maester:

    def_dbdir = Path('yaatdb_local')
    db_name: str = 'yaatdb'
    candles_db_name: str = 'candlesdb'

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
        self.candles_db = self.dbc[self.candles_db_name]

        # create collections

        self.informers = self.init_collection('informers', self.informers_schema)
        self.predictions = self.init_collection('predictions', self.predictions_schema)

        # create indexes

        self.informers.create_index({'name':1}, unique=True)
        self.predictions.create_index({'name':1}, unique=True)

        # file store

        self.fs = gridfs.GridFS(self.db)

        # data api

        self.polygon = polygon.RESTClient(api_key=self.polygon_key)

    # database config

    def init_collection(self, name, schema, db=None) -> Collection:
        db = db if db is not None else self.db
        if name in db.list_collection_names():
            db.command('collMod', name, validator={'$jsonSchema': schema})
        else:
            db.create_collection(name, validator={'$jsonSchema': schema})
        return db[name]

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

    def get_dataset(self, tickers:List[str], fields:Optional[List[str]]=None, freq:str='1min',
                    start_date:Optional[datetime]=None, end_date:Optional[datetime]=None) -> pd.DataFrame:
        # projection and sort stage
        proj_sort_stage =  [{'$project': {'_id': 0, **({field:1 for field in fields + ['date']} if fields is not None else {})}},
                            {'$sort': {'date': 1}}]

        # date stage
        date_conditions = {}
        if start_date: date_conditions['$gte'] = start_date
        if end_date: date_conditions['$lte'] = end_date
        date_stage = [{'$match': {'date': date_conditions}}] if date_conditions else []

        # query and get dataframes
        dfs = {tick: pd.DataFrame(list(self.candles_db[f"{tick}_{freq}"].aggregate(date_stage + proj_sort_stage))) for tick in tickers}

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

    # gets entire months
    def create_tickers_dataset(self, ticker:str, freq:str='1min', start_date:Optional[datetime]=None, end_date:Optional[datetime]=None):
        collname = f"{ticker}_{freq}"
        assert collname not in self.candles_db.list_collection_names(), self.candles_db.list_collection_names()

        # create the collection
        tick_coll = self.init_collection(collname, self.candle_schema, self.candles_db)

        # no duplicate dates
        tick_coll.create_index({'date': 1}, unique=True)

        # get start and end dates

        if start_date is None:
            start_date = self.alpha_get_earliest(ticker, datetime(2015, month=1, day=1)) # start_date heuristic

        if end_date is None:
            end_date = datetime.now()

        # insert data
        dates = list(pd.date_range(start=start_date, end=end_date, freq='MS'))
        with tqdm.tqdm(total=len(dates)) as pbar:
            for date in dates:
                res = self.alpha_call_intraday(ticker, date, freq)
                assert res['Meta Data']['6. Time Zone'] == 'US/Eastern', res

                # get the tickers dataframe
                tickers = pd.DataFrame.from_dict(self.alpha_extract_data(res), orient='index')
                colnames = tickers.columns
                tickers.reset_index(inplace=True)
                tickers.rename(columns={**{'index': 'timestamp'}, **{name:name.split(' ')[1] for name in colnames}}, inplace=True)

                # process timezone
                tickers['date'] = pd.to_datetime(tickers['timestamp'], errors='raise').dt.tz_localize('America/New_York', ambiguous='raise')

                # set the datatypes
                tickers[floatcols] = tickers[floatcols:=['open', 'close', 'high', 'low']].astype(float)
                tickers['volume'] = tickers['volume'].astype(int)

                # drop the timestamp column
                tickers.drop('timestamp', axis=1, inplace=True)

                # insert
                pbar.set_postfix(status=date)
                try: self.candles_db[collname].insert_many(tickers.to_dict('records'))
                except Exception as e:
                    print("---- Exception encountered ----")
                    print(e)
                pbar.update(1)
                

    # alphavantage

    def alpha_extract_data(self, res): return  res[(set(res.keys()) - {'Meta Data'}).pop()]

    def alpha_call_intraday(self, ticker:str, date:datetime, freq:str='1min'):
        return self.alpha_call(function='TIME_SERIES_INTRADAY', outputsize='full', extended_hours='true', interval=freq, symbol=ticker, month=f"{date.year}-{date.month:02}")

    def alpha_call(self, **kwargs):
        # construct the url
        url = self.alpha_url + ''.join(map(lambda kv: kv[0] + '=' + str(kv[1]) + '&', kwargs.items())) + f'apikey={self.alpha_key}'

        # call it (with rate limit governance)
        start = time.time()
        while (time.time() - start) < 62: # if we cant call after a minute there was an issue
            if 'Information' not in (data:=fetchjson(url)):
                assert 'Error Message' not in data.keys(), f"{data} \n\n {url}"
                return data
            if "higher API call volume" not in data['Information']: raise RuntimeError(data)
        raise RuntimeError(data)

    # NOTE slop
    def alpha_get_earliest(self, ticker:str, start_date:Optional[datetime]=None) -> datetime:
        curr_date = datetime.strptime('2020-01', '%Y-%m') if start_date is None else start_date
        while True:
            try: res = self.alpha_call_intraday(ticker, curr_date, freq='60min')
            except AssertionError as e: break
            candles = res['Time Series (60min)']
            response_date = datetime.strptime(list(candles.keys())[0], '%Y-%m-%d %H:%M:%S')
            if curr_date.year == response_date.year and curr_date.month == response_date.month: curr_date = curr_date - timedelta(days=15)
            else: break
        good_date = curr_date + timedelta(days=15)
        try: res = self.alpha_call_intraday(ticker, good_date, freq='60min')
        except Exception as e:
            print("proably bad start date")
            raise e
        candles = res['Time Series (60min)']
        response_date = datetime.strptime(list(candles.keys())[0], '%Y-%m-%d %H:%M:%S')
        assert good_date.year == response_date.year and good_date.month == response_date.month, f"{good_date}, {response_date} - probably bad start_date"
        return good_date

    