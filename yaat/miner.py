from __future__ import annotations
from yaat.util import myprint
from typing import TYPE_CHECKING
import yfinance as yf, pandas as pd
import requests
if TYPE_CHECKING:
    from yaat.maester import Maester

class Miner:
    def __init__(self, maester: Maester):
        self.maester = maester

    def mine(self):

        import requests

        # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
        url = 'https://www.alphavantage.co/query?function=MARKET_STATUS&apikey=demo'
        r = requests.get(url)
        data = r.json()

        myprint('blah', data)

        ## need to fetch historical data in 60 day increments??

        data_avail_hour = '2022-04-01'

        # Define the ticker
        ticker = 'BTC-USD'

        # Fetch the data
        data = yf.download(tickers=ticker, start='2022-04-01', interval='1h')

        # Check the first few rows to confirm
        print(len(data))

        # symbols = ['^GSPC', '^IXIC', '^DJI', 'BTC-USD', 'ETH-USD', 'DX-Y.NYB',
        #            'EURUSD=X', 'USDCNY=X', 'GC=F', 'CL=F', '^SPNY']
        # tickers = yf.Tickers(' '.join(symbols))
        # print('\n')

        # myprint('here', tickers.tickers['^GSPC'].info)