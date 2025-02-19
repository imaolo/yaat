import requests, time, tqdm, pandas as pd 
from datetime import timedelta, datetime
from yaat.maester import Maester

tick = 'SPY'

maester = Maester(connstr='mongodb://Earl:pink-Flamingo1317@52.91.137.11/')

alpha_url = 'https://www.alphavantage.co/query?'
alpha_key = '3KZFIF8WVK43Q92B'

def fetchjson(url:str): return requests.get(url).json()

def alpha_get_data(res): return  res[(set(res.keys()) - {'Meta Data'}).pop()]

def call_alpha(**kwargs):
    # construct the url
    url = alpha_url + ''.join(map(lambda kv: kv[0] + '=' + str(kv[1]) + '&', kwargs.items())) + f'apikey={alpha_key}'

    # call it (with rate limit governance)
    start = time.time()
    while (time.time() - start) < 62: # if we cant call after a minute there was an issue
        if 'Information' not in (data:=fetchjson(url)):
            assert 'Error Message' not in data.keys(), f"{data} \n\n {url}"
            return data
        if "higher API call volume" not in data['Information']: raise RuntimeError(data)
    raise RuntimeError(data)

def call_alpha_date(date): return call_alpha(function='TIME_SERIES_INTRADAY', outputsize='full', extended_hours='true', interval=f'{1}min', symbol=tick, month=f"{date.year}-{date.month:02}")

# insert
dates = list(pd.date_range(start=datetime(2000, month=1, day=1), end=datetime.now(), freq='MS'))
for date in tqdm.tqdm(dates):
    res = call_alpha_date(date)
    assert res['Meta Data']['6. Time Zone'] == 'US/Eastern', res

    # get the tickers dataframe
    tickers = pd.DataFrame.from_dict(alpha_get_data(res), orient='index')
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
    print("inserting date: ", date)
    try: maester.spy_1min_ohclv.insert_many(tickers.to_dict('records'))
    except Exception as e:
        print("---- Exception encountered ----")
        print(e) 

    

# get oldest date

if False:
    desired_date = datetime.strptime('2001-01', '%Y-%m')
    while True:
        res = call_alpha_date(desired_date)
        candles = res['Time Series (1min)']
        response_date = datetime.strptime(list(candles.keys())[0], '%Y-%m-%d %H:%M:%S')
        if desired_date.year == response_date.year and desired_date.month == response_date.month: desired_date = desired_date - timedelta(days=15)
        else: break

    print(desired_date + timedelta(days=15))