from yaat.util import killproc, fetchjson, myprint, DEBUG
from dataclasses import dataclass
from datetime import date, time, datetime
from pymongo import MongoClient
from subprocess import Popen, DEVNULL
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import atexit, functools, pandas as pd, pymongo.errors as mongoerrs, pandas_market_calendars as mcal

DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'

@dataclass
class TimeRange:
    start: date
    end: date
    times: List[time]
    calendar_name: str = 'NYSE'

    def __post_init__(self):
        self.start = self.clean_date(self.start)
        self.end = self.clean_date(self.end)
        if self.start > self.end: raise RuntimeError(f"start date cannot be after end date {self.start}, {self.end}")

        if not isinstance(self.times, list): raise RuntimeError(f"times must be a list type {self.times}")
        self.times = list(map(self.clean_time, self.times))
        self.calendar = mcal.get_calendar(self.calendar_name)

        self.days: pd.DatetimeIndex = self.calendar.schedule(self.start, self.end).index

        self.timestamps: pd.DatetimeIndex = self.days.repeat(len(self.times)) + pd.to_timedelta(self.times * len(self.days))

    @staticmethod
    def clean_date(d: date | str) -> str:
        if isinstance(d, date): return d.strftime(DATE_FORMAT)
        if isinstance(d, str): return datetime.strptime(d, DATE_FORMAT).date().strftime(DATE_FORMAT)
        raise RuntimeError(f"invalid date {d}")

    @staticmethod
    def clean_time(t: time | str) -> str:
        if isinstance(t, time):
            if t.tzinfo is not None: raise RuntimeError(f"time cannot have timezone {t}")
            return t.strftime(TIME_FORMAT)
        if isinstance(t, str): return datetime.strptime(t, TIME_FORMAT).time().strftime(TIME_FORMAT)
        raise RuntimeError(f"invalid time {t}")

class Maester:
    db_name: str = 'yaatdb'
    alpha_key:str='LLE2E6Y7KG1UIS8R'
    alpha_url: str = 'https://www.alphavantage.co/query?'

    tickers_schema: Dict = {
        'title': 'OHCL(V) stock, currency, and crypto currency tickers (currencies in USD)',
        'required': ['symbol', 'timestamp', 'open', 'close', 'high', 'low', 'volume'],
        'properties': {
            'symbol':     {'bsonType': 'string'},
            'timestamp':  {'bsonType': 'date'},
            'open':       {'bsonType': 'double'},
            'close':      {'bsonType': 'double'},
            'high':       {'bsonType': 'double'},
            'low':        {'bsonType': 'double'},
            'volume':     {'bsonType': ['int', 'null']}
        }
    }

    # construction

    def __new__(cls, connstr:Optional[str]=None, dbdir:Optional[Path | str]=None):
        if connstr is not None: 
            if dbdir is not None: raise RuntimeError('cannot specify a connection string and to start a local database')
        else:
            try:
                cls.conndb()
                raise RuntimeError('Local database already started(1)')
            except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure): pass
        return super().__new__(cls)

    def __init__(self, connstr:Optional[str]=None, dbdir:Optional[Path | str]=None):
        # clean and store arguments
        self.connstr = connstr if connstr is not None else 'localhost:27017'
        if connstr is not None: self.dbdir = None
        else: self.dbdir = Path(dbdir) if dbdir is not None else Path('yaatdb_local')

        # connect db (None connection string means start the database - connect via localhost)
        self.dbc, self.mongo_proc = self.startlocdb(self.dbdir) if connstr is None else (self.conndb(self.connstr), None)

        # get the database from the client connection
        self.db = self.dbc[self.db_name]

        # helper
        def create_collection(name, schema):
            if name in self.db.list_collection_names(): return self.db[name]
            else: return self.db.create_collection(name, validator={'$jsonSchema': schema})

        # create the tickers collection
        self.tickers = create_collection('tickers', self.tickers_schema)
        self.tickers.create_index({'symbol':1, 'timestamp':1}, unique=True)
        self.tickers.create_index({'symbol':1})
        self.tickers.create_index({'timestamp':1})

    # database

    @classmethod
    def conndb(cls, url:Optional[str]=None) -> MongoClient:
        (c := MongoClient(url, serverSelectionTimeoutMS=5000))['admin'].command('ping')
        return c

    @classmethod
    def startlocdb(cls, dbdir:Path) -> Tuple[MongoClient, Popen]:
        dbdir.mkdir(parents=True, exist_ok=True)
        try: 
            cls.conndb()
            assert False, 'Local database already started(2)'
        except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure):
            try: mongo_proc = Popen(['mongod', '--dbpath', str(dbdir.absolute())], stdout=DEVNULL, stderr=DEVNULL) # NOTE: --logpath
            except Exception as e: print(f"mongod failed: {e}"); raise
            atexit.register(functools.partial(killproc, mongo_proc))
            return cls.conndb(), mongo_proc

    def __del__(self):
        if self.mongo_proc is not None: killproc(self.mongo_proc)

    # query helpers

    @staticmethod
    def get_ts_agg(tr: TimeRange) -> List[Dict]:
        return  [
            {'$addFields':{
                'just_date': {'$dateToString': {'format': DATE_FORMAT, 'date': '$timestamp'}},
                'just_time': {'$dateToString': {'format': TIME_FORMAT, 'date': '$timestamp'}}
            }},
            {'$match': {
                'just_time': {'$in': tr.times},
                'just_date': {'$in': tr.days.strftime(DATE_FORMAT).to_list()}
            }},
            {'$project': {'just_time': 0, 'just_date': 0}}
        ]

    # alpha vantage functions

    @staticmethod
    def alpha_get_tz(res: Dict) -> str: return res['Meta Data']['6. Time Zone']

    @staticmethod
    def alpha_get_data(res: Dict) -> Dict: return  res[(set(res.keys()) - {'Meta Data'}).pop()]

    @staticmethod
    def alpha_check_freq_min(freq_min:int):
        if freq_min not in (1, 5, 15, 30, 60): raise RuntimeError(f"{freq_min} is not a valid minute interval. Valid are 1, 5, 15, 30, 60.")

    @classmethod
    def alpha_get_times(cls, freq_min:int) -> List[time]:
        cls.alpha_check_freq_min(freq_min)
        res = cls.alpha_call(function='TIME_SERIES_INTRADAY', symbol='IBM', interval=f'{freq_min}min', extended_hours='false', month='2022-01', outputsize='full')
        return list(pd.unique(pd.DatetimeIndex(cls.alpha_get_data(res).keys()).tz_localize(cls.alpha_get_tz(res)).tz_convert('UTC').time))

    def alpha_mine(self, start:date, end: date, sym: str, freq_min:int):
        self.alpha_check_freq_min(freq_min)

        # create the time range (make sample api call and get the unique times from it)
        res = self.alpha_call(function='TIME_SERIES_INTRADAY', symbol='IBM', interval=f'{freq_min}min', extended_hours='false', month='2022-01', outputsize='full')
        times = pd.unique(pd.DatetimeIndex(self.alpha_get_data(res).keys()).tz_localize(self.alpha_get_tz(res)).tz_convert('UTC').time)
        tr = TimeRange(start, end, list(times))

        # get existing tickers
        existing_tickers = pd.DataFrame(list(self.tickers.aggregate([{'$match': {'symbol': sym}}] + self.get_ts_agg(tr))))

        # get missing timestamps
        missing_ts = tr.timestamps.difference(pd.DatetimeIndex(existing_tickers['timestamp'])) if len(existing_tickers) > 0 else tr.timestamps
        missing_mys = missing_ts.to_period('M').unique().strftime('%Y-%m')

        # insert the missing tickers
        for my in missing_mys:
            # make api call
            res = self.alpha_call(function='TIME_SERIES_INTRADAY', symbol=sym, interval=f'{freq_min}min', extended_hours='false', month=my, outputsize='full')

            # get tickers as dataframe
            tickers = pd.DataFrame.from_dict(self.alpha_get_data(res), orient='index')
            ohlcv_names = tickers.columns
            tickers.reset_index(inplace=True)
            tickers.rename(columns={**{'index': 'timestamp'}, **{name:name.split(' ')[1] for name in ohlcv_names}}, inplace=True)

            # process timezone
            tickers['timestamp'] = pd.to_datetime(tickers['timestamp'], errors='raise').dt.tz_localize(self.alpha_get_tz(res), ambiguous='raise').dt.tz_convert('UTC').dt.tz_localize(None, ambiguous='raise')

            # get only the missing tickers
            tickers = tickers[tickers['timestamp'].isin(missing_ts)]

            # set the datatypes
            tickers[floatcols] = tickers[floatcols:=['open', 'close', 'high', 'low']].astype(float)
            tickers['volume'] = tickers['volume'].astype(int)

            # insert
            tickers['symbol'] = sym
            self.tickers.insert_many(tickers.to_dict('records'))

    @classmethod
    def alpha_call(cls, **kwargs) -> Dict:
        # construct the url
        url = cls.alpha_url + ''.join(map(lambda kv: kv[0] + '=' + str(kv[1]) + '&', kwargs.items())) + f'apikey={cls.alpha_key}'
        if DEBUG: print(f"call alpha: {url}")

        # call it (with rate limit governance)
        start = None
        while start is None or (start is not None and (time() - start) < 62): # 75req/min
            if 'Information' not in (data:=fetchjson(url)):
                assert 'Error Message' not in data.keys(), f"{data} \n\n {url}"
                if DEBUG: myprint("called alpha", data)
                return data
            if "higher API call volume" not in data['Information']: raise RuntimeError(data)
            if start is None: start = time()
        raise RuntimeError(data)
