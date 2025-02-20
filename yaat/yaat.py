from pymongo import MongoClient
from yaat.mongo import MongoCollection, MongoDoc
from typing import final
from apscheduler.schedulers.blocking import BlockingScheduler
from abc import ABC, abstractmethod
from datetime import datetime
from wsgiref.simple_server import make_server;
from http.server import HTTPServer, BaseHTTPRequestHandler
import os, requests, asyncio

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

    def __call__(self, url: str, headers: dict | None = None, **kwargs) -> dict:
        return requests.get(url, headers=headers, params=kwargs).json()

    @abstractmethod
    def scrape(self):
        pass

class CoinGeckoScraper(Scraper, ABC):
    api_url = "https://api.coingecko.com/api/v3/"

    def __init__(self, coll:MongoCollection, api_key:str=COINGECKO_KEY):
        self.mcoll = coll
        self.headers = {"accept": "application/json", "x-cg-demo-api-key": api_key}

    def __call__(self, cmd:str, **kwargs) -> dict:
        return super().__call__(self.api_url + cmd, headers=self.headers, **kwargs)

class TopMoversScraper(CoinGeckoScraper):
    collname: str = 'top_movers'
    durations: list[str] = ['1h', '24h', '7d', '14d', '30d', '1y']
    top_coins: list[str] = ['300', '500', '1000', 'all']

    def __init__(self, dbc:MongoClient, api_key:str=COINGECKO_KEY):
        super().__init__(MongoCollection(dbc[self.dbname][self.collname], TopMoverDoc), api_key)

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

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Hello, world from CICD#2!")

def run():
    dbc = MongoClient(host='mongo')

    # create the listeners
    hb_listener = lambda:  make_server("0.0.0.0", 8000, lambda env, res: (res('200 OK', [('Content-type', 'text/plain; charset=utf-8')]), [b"OK"])[1]).serve_forever(poll_interval=0.1)
    web_listener = lambda:  HTTPServer(('0.0.0.0', 80), SimpleHandler).serve_forever(poll_interval=0.1)

    # create the schedule and add the jobs
    scheduler = BlockingScheduler()
    scheduler.add_job(TopMoversScraper(dbc).scrape, 'interval', seconds=1)
    scheduler.add_job(hb_listener, 'date', run_date=datetime.now())
    scheduler.add_job(web_listener, 'date', run_date=datetime.now())

    # start the schedule
    scheduler.start()
