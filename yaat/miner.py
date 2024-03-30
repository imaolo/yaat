from __future__ import annotations
from yaat.util import myprint
from typing import TYPE_CHECKING, Dict
import requests
if TYPE_CHECKING:
    from yaat.maester import Maester

class Miner:
    alpha_url: str = 'https://www.alphavantage.co/query?'
    alpha_key: str = 'G9JFJ30E4HK6X0HR'

    def __init__(self, maester: Maester):
        self.maester = maester
        self.url = self.alpha_url

    def add_url_qfield(self, key:str, val:str):  self.url = ''.join([self.url, key + '=' + val + '&'])

    def get(self) -> Dict: return requests.get(self.url+f'apikey={self.alpha_key}').json()

    def mine(self):
        self.add_url_qfield('function', 'TIME_SERIES_INTRADAY')
        self.add_url_qfield('symbol', 'IBM')
        self.add_url_qfield('interval', '60min')
        myprint('alpha advantage demo data', self.get())