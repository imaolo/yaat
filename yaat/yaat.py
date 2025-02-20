from pymongo import MongoClient
from yaat.mongo import MongoCollection, MongoDoc
from typing import Callable, final
from apscheduler.schedulers.background import BackgroundScheduler
from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import datetime
from wsgiref.simple_server import make_server;
import os, requests, threading, signal, sys

# https://docs.coingecko.com/reference/coins-top-gainers-losers

class TopMoverQuery(MongoDoc):
    duration: str
    top_coins: str

class TopMoverResult(MongoDoc):
    id: str
    symbol: str
    name: str
    usd: float
    market_cap_rank: int
    usd_24h_vol: int
    usd_1y_change: int

class TopMoverDoc(MongoDoc):
    timestamp: datetime
    query: TopMoverQuery
    result: TopMoverResult

COINGECKO_KEY = os.environ['COINGECKO_KEY']

class Scraper(ABC):
    dbname: str = 'scraper_db'

    def __init__(self, mcoll:MongoCollection, interval_sec:int):
        self.mcoll = mcoll
        self.interval_sec = interval_sec

    def __call__(self, url: str, headers: dict | None = None, **kwargs) -> dict:
        return requests.get(url, headers=headers, params=kwargs).json()

    @final
    def schedule_job(self, scheduler:BackgroundScheduler):
        scheduler.add_job(self.scrape, 'interval', seconds=self.interval_sec)

    @abstractmethod
    def scrape(self):
        pass

class CoinGeckoScraper(Scraper, ABC):
    api_url = "https://api.coingecko.com/api/v3/"

    def __init__(self, coll:MongoCollection, interval_sec:int, api_key:str=COINGECKO_KEY):
        super().__init__(coll, interval_sec)
        self.headers = {"accept": "application/json", "x-cg-demo-api-key": api_key}

    def __call__(self, cmd:str, **kwargs) -> dict:
        return super().__call__(self.api_url + cmd, headers=self.headers, **kwargs)

class TopMoversScraper(CoinGeckoScraper):
    collname: str = 'top_movers'
    durations: list[str] = ['1h', '24h', '7d', '14d', '30d', '1y']
    top_coins: list[str] = ['300', '500', '1000', 'all']

    def __init__(self, dbc:MongoClient, interval_sec:int, api_key:str=COINGECKO_KEY):
        super().__init__(MongoCollection(dbc[self.dbname][self.collname], TopMoverDoc), interval_sec, api_key)

    def __call__(self, **kwargs) -> dict:
        return super().__call__('movers', **kwargs)

    def scrape(self):
        for duration in self.durations:
            for top_coin in self.top_coins:
                query_doc = TopMoverQuery(duration=duration, top_coins=top_coin)
                # TODO - clean
                # coll.insert_many(
                #     [(doc.pop("image"), TopMoverDoc(timestamp=datetime.now(), query=query_doc, results=TopMoverResult(**doc)))[1]
                #      for doc in self.call('/movers', **dict(query_doc))[0]["top_gainers"]])
                self.mcoll.coll.insert_one(
                    self.mcoll.doctype(timestamp=datetime.now(), query=query_doc, result=TopMoverResult(**{
                        'id': 'btc',
                        'symbol': 'btc',
                        'name': 'bitcoin',
                        'usd': 1.0,
                        'market_cap_rank': 1,
                        'usd_24h_vol': 1,
                        'usd_1y_change': 1,
                    })).asdict())

def run():
    dbc = MongoClient(host='mongo')

    # create the schedule 
    scheduler = BackgroundScheduler()

    # add to schedule
    TopMoversScraper(dbc, 1).schedule_job(scheduler)

    # start the schedule
    scheduler.start()

    # start the heardbeat
    make_server("0.0.0.0", 8000, lambda env, res: (res('200 OK', [('Content-type', 'text/plain; charset=utf-8')]), [b"OK"])[1]).serve_forever()
