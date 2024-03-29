from __future__ import annotations
from typing import TYPE_CHECKING
import yfinance as yf, pandas as pd
if TYPE_CHECKING:
    from yaat.maester import Maester

class Miner:
    def __init__(self, maester: Maester):
        self.maester = maester

    def mine(self):
        tickers = ['^GSPC', '^IXIC', '^DJI', 'BTC-USD', 'ETH-USD', 'DX-Y.NYB',
                   'EURUSD=X', 'USDCNY=X', 'GC=F', 'CL=F', '^SPNY']
        for t in tickers: yf.Ticker(t)

# def test_create_dataset(self):
#         cols = {'c1': pd.Series([], dtype='str'), 'c2': pd.Series([], dtype='int'), 'c3': pd.Series([], dtype='float')}
#         data = ['c1 val', 1, 1.3]
#         self.maester.create_dataset(n:=f"{getid(self)}_{gettime()}", cols, mem=0)
#         de = self.maester.datasets[n]
#         de.dataset.buf += data
#         self.assertEqual(de.dataset.data.iloc[0].tolist(), data)