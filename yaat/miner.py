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

    @classmethod
    def get_intervals(cls, freq_min:int, start:datetime, end:datetime, bus_hours:bool) -> pd.DataFrame:
        cls.check_freq_min(freq_min)
        ints = pd.to_datetime(pd.DataFrame(pd.date_range(start=start, end=end, freq=f"{freq_min}min"), columns=['datetime'])['datetime'])
        return ints[ints.apply(lambda dt: Maester.is_business_hours(dt.to_pydatetime()))] if bus_hours else ints

    @classmethod
    def get_int_sym_combos(cls, freq_min:int, start:datetime, end:datetime, syms:List[str], bus_hours:bool) -> pd.DataFrame:
        cls.check_freq_min(freq_min)
        ints = cls.get_intervals(freq_min, start, end, bus_hours)               
        return pd.DataFrame(index=pd.MultiIndex.from_product([ints, syms], names=['datetime', 'symbol'])).reset_index()

    def get_missing_tickers(self, freq_min:int, start:datetime, end:datetime, syms:List[str], bus_hours:bool) -> List[missing_ticker_class]:
        self.check_freq_min(freq_min)

        # get existing and interval/symbol combinations
        existing = pd.DataFrame(self.get_existing_tickers(freq_min, start, end, syms), columns=['datetime', 'symbol'])
        existing['datetime'] = pd.to_datetime(existing['datetime']) 
        ints_syms_combos = self.get_int_sym_combos(freq_min, start, end, syms, bus_hours)

        # merge & filter to get missing
        merged = pd.merge(ints_syms_combos, existing, on=['datetime', 'symbol'], how='left', indicator=True)
        missing = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)

        return [self.missing_ticker_class(**r.to_dict()) for _, r in missing.iterrows()]

    # def mine_alpha(self, freq_min:int=15, start:datetime=datetime(2023, 2, 2, tzinfo=Maester.tz), end:datetime=datetime(2023, 4, 2, tzinfo=Maester.tz),
    #                syms:List[str]=list({'SPY', 'XLK', 'XLV', 'XLY', 'IBB', 'XLF', 'XLP', 'XLE', 'XLU', 'XLI','XLB'})) -> List[missing_ticker_class]:
    #     self.check_freq_min(freq_min)

    #     # parameter cleaning
    #     syms = list(set(syms))
    #     start = start.astimezone(Maester.tz).replace(tzinfo=None)
    #     end = end.astimezone(Maester.tz).replace(tzinfo=None)
    #     if DEBUG: print(f"freq_min: {freq_min}, start: {start}, end: {end}, syms: {syms}")

    #     # get misssing symbols and datetimes by month and year
    #     missing = pd.DataFrame(self.get_missing_tickers(freq_min, start, end, syms))
    #     missing['year'] = missing['datetime'].dt.year
    #     missing['month'] = missing['datetime'].dt.month
    #     missing = missing.groupby(['symbol', 'year', 'month']).agg({'datetime': list}).reset_index()

    #     inserted: List[self.missing_ticker_class] = []
    #     for _, m in missing.iterrows():
    #         myprint('HERE', m['datetime'])
    #         res = self.call_alpha(function='TIME_SERIES_INTRADAY', outputsize='full', interval=f'{freq_min}min', symbol=m['symbol'], month=f"{m['year']}-{m['month']:02}")
    #         assert len(res.keys()) == 2

    #         metadata = res['Meta Data']
    #         assert metadata['2. Symbol'] == m['symbol']
    #         sym = m['symbol']

    #         tickers: Dict = res[(set(res.keys()) - set(['Meta Data'])).pop()]
    #         for time, ohlcv in tickers.items():
    #             if pd.Timestamp(time) in m['datetime']:
    #                 ohlcv = {(lambda k: k.split(' ')[1])(k): float(v) for k, v in ohlcv.items()}
    #                 if 'volume' in ohlcv.keys(): ohlcv['volume'] = int(ohlcv['volume'])
    #                 dt = datetime.strptime(time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=ZoneInfo(metadata['6. Time Zone'])).astimezone(Maester.tz)
    #                 self.insert_ticker(Maester.ticker_class(sym, dt, **ohlcv))
    #                 inserted.append(self.missing_ticker_class(sym, dt))
    #             else: print(time)
    #         return inserted

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
                assert 'Error Message' not in data.keys(), data
                if DEBUG: myprint("called alpha", data)
                return data
            if "higher API call volume" not in data['Information']: raise RuntimeError(data)
            if start is None: start = time.time()
        raise RuntimeError(data)