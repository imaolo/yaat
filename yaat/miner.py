from __future__ import annotations
from yaat.util import fetchjson, currdt, killproc, path, mkdirs, ROOT, DBNAME
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
from datetime import datetime
from bson.dbref import DBRef
from typing import Any, Optional, List, Dict, Tuple
from pymongo.collection import Collection
from pymongo.database import Database
import time, functools, atexit, subprocess, pymongo, pymongo.errors as mongoerrs

## the miner handles the mine and the depo
# the mine gets the data and the depo stores the data
# the miner assists in managing these functionalities

# TODO - test depo

class Depo:
    def __init__(self, db_name:str=DBNAME, dir:str=ROOT, connstr:Optional[str]='mongodb://54.205.245.140:27017/'):
        self.db_name, self.dir, self.connstr = db_name, dir, connstr if connstr else 'mongodb://localhost:27017/'
        self.db: Database = (self.conndb(connstr) if connstr else self.startlocdb(path(dir, db_name)))['yaatdb']

        self.stocks_tickers, self.stocks_metadata = self.create_colls('stocks')
        self.currs_tickers, self.currs_metadata = self.create_colls('currs')

    def create_colls(self, name: str) -> Tuple[Collection, Collection]:
        t, t_meta = self.db[name+'_tickers'], self.db[name+'_metadata']
        t.create_index({'datetime':1}, unique=True)
        t_meta.create_index({'metadata':1}, unique=True)
        return t, t_meta
        
    def insert_metadata(self, coll: Collection, data: Dict) -> DBRef:
        query, update = {'metadata': data.pop('Meta Data')}, {'$setOnInsert': {'datetime': currdt()}}
        result = coll.update_one(query, update, upsert=True)
        refcons = functools.partial(DBRef, coll.name, database=self.db.name)
        return refcons(result.upserted_id) if result.upserted_id else refcons(coll.find_one(query)['_id'])

    @classmethod
    def conndb(cls, url:Optional[str]=None) -> MongoClient:
        (c := pymongo.MongoClient(url, serverSelectionTimeoutMS=5000))['admin'].command('ping')
        return c

    @classmethod
    def startlocdb(cls, dir:str) -> MongoClient:
        mkdirs(dir)
        try: return cls.conndb()
        except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure):
            mongod = subprocess.Popen(['mongod', '--dbpath', dir], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) # NOTE: --logpath
            atexit.register(functools.partial(killproc, mongod, 'mongod'))
            return cls.conndb()

class Miner:
    alpha_url: str = 'https://www.alphavantage.co/query?'

    def __init__(self, alpha_key:str='LLE2E6Y7KG1UIS8R', mongo_connstr:Optional[str]='mongodb://54.205.245.140:27017/'):
        self.alpha_key = alpha_key
        self.depo = Depo(mongo_connstr)

    def scalp_alpha(self, symbol:str, **kwargs):
        data = self.call_alpha(self.alpha_key, symbol=symbol, **kwargs)
        ref = self.depo.insert_metadata(self.depo.stockst_meta, data)

        assert len(data.items()) == 1
        data: Dict[str, Dict]= list(data.values())[0]

        symref = {'symbol':symbol, 'metadata': ref}
        self.depo.stockst.insert_many([symref | {'datetime': datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')} |
                                      {k[3:]:float(v) for k, v in ohlcv.items()}
                                        for dt, ohlcv in data.items()])

    # TODO - cannot always go continuously
    # TODO - default arguments can go continusouly, pass parameters to stop and start
    # TODO - prepare for threading
    # TODO - general parameterization
    # TODO - mine_stocks, mine_currs, mine_sentiment

    def mine(self, stock_symbols:List[str]=[], currency_symbols:List[str]=[]):
        stock_symbols = ['SPY', 'XLK', 'XLV', 'XLY', 'IBB', 'XLF', 'XLP', 'XLE', 'XLU', 'XLI','XLB'] # 'XLRE'
        currency_symbols = ['BTC-USD', 'ETH-USD', 'DX-Y.NYB', 'EURUSD=X', 'USDCNY=X', 'GC=F', 'CL=F']

        curr_date = datetime.now() + relativedelta(months=1) # loop skips first month
        stop_date = curr_date - relativedelta(months=10) # todo, when to start
        for date in iter(lambda: (curr_date := curr_date - relativedelta(months=1)), stop_date):
            for sym in stock_symbols:
                self.scalp_vantage(sym, function='TIME_SERIES_INTRADAY', interval='60min', 
                                   month=date.strftime('%Y-%m'), datatype='json', outputsize='full')

    @classmethod
    def call_alpha(cls, apikey:str, **kwargs) -> Any:
        url = cls.alpha_url + ''.join(map(lambda kv: kv[0] + '=' + kv[1] + '&', kwargs.items())) + f'&apikey={apikey}'
        start = None
        while start is None or (start is not None and (time.time() - start) < 62): # 75req/min
            if 'Information' not in (data:=fetchjson(url)): return data
            if "higher API call volume" not in data['Information']: raise RuntimeError(data)
            if start is None: start = time.time()
        raise RuntimeError(data)
        
# TIME_SERIES_INTRADAY
# {'Meta Data': {'1. Information': 'Intraday (60min) open, high, low, close '
#                                  'prices and volume',
#                '2. Symbol': 'SPY',
#                '3. Last Refreshed': '2024-03-28 20:00:00',
#                '4. Interval': '60min',
#                '5. Output Size': 'Full size',
#                '6. Time Zone': 'US/Eastern'},
#  'Time Series (60min)': 
#                         {
#                                   : {'1. open': '507.6510',
#                                                       '2. high': '508.0720',
#                                                       '3. low': '507.4210',
#                                                       '4. close': '507.6830',
#                                                       '5. volume': '36275'},
#                               '2024-03-01 05:00:00': {'1. open': '507.6710',
#                                                       '2. high': '507.6730',
#                                                       '3. low': '505.7860',
#                                                       '4. close': '506.3370',
#                                                       '5. volume': '106019'
#                           },