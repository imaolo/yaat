from __future__ import annotations
from yaat.util import myprint
from typing import TYPE_CHECKING, Dict
import requests
if TYPE_CHECKING:
    from yaat.maester import Maester

class Miner:
    alpha_url: str = 'https://www.alphavantage.co/query?'

    def __init__(self, maester: Maester):
        self.maester = maester
        self.url = self.alpha_url

    def add_url_qfield(self, key:str, val:str): self.url = self.url + key + '=' + val + '&'

    def get(self) -> Dict: return requests.get(self.url+'apikey=demo').json()

    def mine(self):
        self.add_url_qfield('function', 'MARKET_STATUS')
        myprint('alpha advantage demo data', self.get())