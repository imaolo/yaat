from yaat.util import fetchjson, myprint, DEBUG
from yaat.maester2 import Maester
from typing import List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo
from pymongo.errors import DuplicateKeyError
import time, pandas as pd

# TODO - debug info

class Miner:
    alpha_url: str = 'https://www.alphavantage.co/query?'
    alpha_key: str = 'LLE2E6Y7KG1UIS8R'

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

    @classmethod
    def mine_alpha(cls, freq_min:int=15, start:datetime=datetime(2023, 2, 2, tzinfo=Maester.tz), end:datetime=datetime(2023, 4, 2, tzinfo=Maester.tz),
                   syms:List[str]=list({'SPY', 'XLK', 'XLV', 'XLY', 'IBB', 'XLF', 'XLP', 'XLE', 'XLU', 'XLI','XLB'})):
        assert freq_min in (1, 5, 15, 30, 60), "valid minute intervals are 1, 5, 15, 30, 60"

        # parameter cleaning
        syms = list(set(syms))
        start = start.astimezone(Maester.tz).replace(tzinfo=None)
        end = end.astimezone(Maester.tz).replace(tzinfo=None)
        if DEBUG: print(f"freq_min: {freq_min}, start: {start}, end: {end}, syms: {syms}")

        # find what is already in the db
        existing_ticks = list(Maester.tickers_coll.aggregate([
            {'$match': {
                'datetime': {'$gte': start, '$lte': end},
                # 'symbol': {'$in': syms},
                # '$expr': {'$and': [
                #     {'$eq': [{'$second': '$datetime'}, 0]},
                #     {'$in': [{'$minute': '$datetime'}, list(range(0, 60, freq_min))]}
                # ]}
            }},
        ]))
        if DEBUG: print(f"number of existing tickers: {len(existing_ticks)}")
        import sys
        sys.exit()

        # get missing symbols and datetimes
        missing_ticks: List[Dict] = []
        desired_ints = set(pd.date_range(start=start, end=end, freq=f"{freq_min}min"))
        existing_ticks_df = pd.DataFrame(existing_ticks, columns=['datetime', 'symbol'])
        for sym in syms:
            sym_ints = set(existing_ticks_df[existing_ticks_df['symbol'] == sym]['datetime'])
            missing_ints = desired_ints - sym_ints
            for interv in missing_ints: missing_ticks.append({'symbol': sym, 'datetime': interv})
        if DEBUG: print(f"number of missing tickers: {len(missing_ticks)}")

        # get misssing symbols and datetimes by month and year
        missing_ticks = pd.DataFrame(missing_ticks)
        missing_ticks['year'] = missing_ticks['datetime'].dt.year
        missing_ticks['month'] = missing_ticks['datetime'].dt.month
        missing_ticks = missing_ticks.groupby(['symbol', 'year', 'month']).agg({'datetime': list}).reset_index()

        # retrieve and insert
        for _, mt in missing_ticks.iterrows():
            if DEBUG: print("processing row"); print(mt); myprint("missing ticker datetime", mt['datetime'])
            res = cls.call_alpha(function='TIME_SERIES_INTRADAY', outputsize='full', interval=f'{freq_min}min', symbol=mt['symbol'], month=f"{mt['year']}-{mt['month']:02}")
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