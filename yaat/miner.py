from yaat.util import fetchjson, myprint, DEBUG
from yaat.maester import Maester
from typing import List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo
from dataclasses import asdict, dataclass
import time, pandas as pd


class Miner:
    alpha_url: str = 'https://www.alphavantage.co/query?'
    alpha_key: str = 'LLE2E6Y7KG1UIS8R' # TODO - move to constructor

    @dataclass
    class missing_ticker_class:
        symbol: int
        datetime: datetime

    def __init__(self, maester:Maester):
        self.maester = maester

    def insert_ticker(self, ticker:Maester.ticker_class): self.maester.tickers_coll.insert_one(asdict(ticker))

    def get_existing_tickers(self, freq_min:int, start:datetime, end:datetime, syms:List[str]) -> List[Dict]:
        self.check_freq_min(freq_min)
        return list(self.maester.tickers_coll.aggregate([
            {'$match': {
                'datetime': {'$gte': start, '$lte': end},
                'symbol': {'$in': syms},
                '$expr': {'$and': [
                    {'$eq': [{'$second': '$datetime'}, 0]},
                    {'$in': [{'$minute': '$datetime'}, list(range(0, 60, freq_min))]}
                ]}
            }},
        ]))

    def get_int_sym_combos(self, freq_min:int, start:datetime, end:datetime, syms:List[str]) -> pd.DataFrame:
        self.check_freq_min(freq_min)
        ints = pd.to_datetime(pd.DataFrame(pd.date_range(start=start, end=end, freq=f"{freq_min}min"), columns=['datetime'])['datetime'])
        return pd.DataFrame(index=pd.MultiIndex.from_product([ints, syms], names=['datetime', 'symbol'])).reset_index()

    def get_missing_tickers(self, freq_min:int, start:datetime, end:datetime, syms:List[str]) -> List[missing_ticker_class]:
        self.check_freq_min(freq_min)

        # get existing and interval/symbol combinations
        existing = pd.DataFrame(self.get_existing_tickers(freq_min, start, end, syms), columns=['datetime', 'symbol'])
        existing['datetime'] = pd.to_datetime(existing['datetime']) 
        ints_syms_combos = self.get_int_sym_combos(freq_min, start, end, syms)

        # merge & filter to get missing
        merged = pd.merge(ints_syms_combos, existing, on=['datetime', 'symbol'], how='left', indicator=True)
        missing = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)

        return [self.missing_ticker_class(**r.to_dict()) for _, r in missing.iterrows()]

    def mine_alpha(self, freq_min:int=15, start:datetime=datetime(2023, 2, 2, tzinfo=Maester.tz), end:datetime=datetime(2023, 4, 2, tzinfo=Maester.tz),
                   syms:List[str]=list({'SPY', 'XLK', 'XLV', 'XLY', 'IBB', 'XLF', 'XLP', 'XLE', 'XLU', 'XLI','XLB'})):
        self.check_freq_min(freq_min)

        # parameter cleaning
        syms = list(set(syms))
        start = start.astimezone(Maester.tz).replace(tzinfo=None)
        end = end.astimezone(Maester.tz).replace(tzinfo=None)
        if DEBUG: print(f"freq_min: {freq_min}, start: {start}, end: {end}, syms: {syms}")

        # find what is already in the db
        existing_ticks = self.get_existing_tickers(freq_min, start, end, syms)
        if DEBUG: print(f"number of existing tickers: {len(existing_ticks)}")

        # get missing symbols and datetimes
        missing_ticks = self.get_missing_tickers()
        if DEBUG: print(f"number of missing tickers: {len(missing_ticks)}")

        # get misssing symbols and datetimes by month and year
        missing_ticks = pd.DataFrame(missing_ticks)
        missing_ticks['year'] = missing_ticks['datetime'].dt.year
        missing_ticks['month'] = missing_ticks['datetime'].dt.month
        missing_ticks = missing_ticks.groupby(['symbol', 'year', 'month']).agg({'datetime': list}).reset_index()

        # retrieve and insert
        for _, mt in missing_ticks.iterrows():
            if DEBUG: print("processing row"); print(mt); myprint("missing ticker datetime", mt['datetime'])
            res = self.call_alpha(function='TIME_SERIES_INTRADAY', outputsize='full', interval=f'{freq_min}min', symbol=mt['symbol'], month=f"{mt['year']}-{mt['month']:02}")
            assert len(res.keys()) == 2
            metadata = res['Meta Data']
            assert metadata['2. Symbol'] == mt['symbol']
            tickers: Dict = res[(set(res.keys()) - set(['Meta Data'])).pop()]
            for time, ohlcv in tickers.items():
                dt = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=ZoneInfo(metadata['6. Time Zone'])).astimezone(Maester.tz)
                if pd.Timestamp(dt) not in mt['datetime']:
                    try:
                        if DEBUG: myprint("inserting(1)", ohlcv); print("inserting(1)", dt)
                        Maester.tickers_coll.insert_one({
                            'datetime': dt,
                            'symbol': mt['symbol'],
                            'open': ohlcv['1. open'],
                            'close': ohlcv['4. close'],
                            'high': ohlcv['2. high'],
                            'low': ohlcv['3. low'],
                            'volume': ohlcv['5. volume']
                        })
                    except Exception as e:
                        print('-'*30, "CATCHING FAILED INSERTION", '-'*30)
                        print("insertion failed - curr datetime", pd.Timestamp(dt))
                        myprint("insertion failed - curr missing", mt['datetime'])
                        myprint("from the db", list(Maester.tickers_coll.find({'datetime': dt, 'symbol': mt['symbol']})))
                        # myprint("insertion failed(1)", ohlcv)
                        # myprint("insertion failed(2)", dt)
                        # myprint("insertion failed(3)", mt['datetime'])
                        # myprint("insertion failed(4)", [pd.Timestamp(dt)])
                        raise e

    @staticmethod
    def check_freq_min(freq_min:int): assert freq_min in (1, 5, 15, 30, 60), "valid minute intervals are 1, 5, 15, 30, 60"

    @classmethod
    def call_alpha(cls, **kwargs) -> Dict:
        # construct the url
        url = cls.alpha_url + ''.join(map(lambda kv: kv[0] + '=' + str(kv[1]) + '&', kwargs.items())) + f'apikey={cls.alpha_key}'
        if DEBUG: print(f"call alpha: {url}")

        # call it (with rate limit governance)
        start = None
        while start is None or (start is not None and (time.time() - start) < 62): # 75req/min
            if 'Information' not in (data:=fetchjson(url)):
                if DEBUG: myprint("called alpha", data)
                return data
            if "higher API call volume" not in data['Information']: raise RuntimeError(data)
            if start is None: start = time.time()
        raise RuntimeError(data)