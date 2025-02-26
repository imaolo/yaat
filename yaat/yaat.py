from yaat.mongo import MongoDoc, MongoCollection, MongoDatabase, MongoInstance
from yaat.helpers import getenv
from apscheduler.schedulers.blocking import BlockingScheduler
from abc import ABC, abstractmethod
from datetime import datetime
from dataclasses import field
from wsgiref.simple_server import make_server
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import ClassVar, Iterator
import requests

DOCKER = getenv('DOCKER', False)

#### CoinGecko ####
class CoinGecko:
    # COINGECKO_KEY = os.environ['COINGECKO_KEY']
    api_url = "https://api.coingecko.com/api/v3/"

    def __init__(self, api_key:str='CG-Qu22wC9h5anAsGR3xt4YiDgR'):
        self.headers = {"accept": "application/json", "x-cg-demo-api-key": api_key}

    def __call__(self, cmd:str='', **kwargs) -> dict:
        return requests.get(self.api_url + cmd, headers=self.headers, params=kwargs).json()
CG = CoinGecko()

#### Abstract Scrapers ####

class ScraperDoc(MongoDoc, ABC):
    timestamp: datetime = field(default_factory=datetime.now)

    @classmethod
    @abstractmethod
    def scrape(cls) -> Iterator[list['ScraperDoc']]:
        pass    

class ScraperCollection(MongoCollection, ABC, doct=ScraperDoc):
    def scrape(self):
        for docs in self.doct.scrape():
            self.insert_many(list(map(lambda d: d.dict, docs)))

#### Scrapers ####
class TopMoverDoc(ScraperDoc):
    # https://docs.coingecko.com/reference/coins-top-gainers-losers
    class QueryDoc(MongoDoc):
        duration: str
        top_coins: str
    class ResultDoc(MongoDoc):
        id: str
        symbol: str
        name: str
        usd: float
        market_cap_rank: int
        usd_24h_vol: int
        usd_1y_change: int

    query: QueryDoc
    result: ResultDoc

    durations: ClassVar[list[str]] = ['1h', '24h', '7d', '14d', '30d', '1y']
    top_coins: ClassVar[list[str]] = ['300', '500', '1000', 'all']

    @classmethod
    def scrape(cls) -> Iterator[list['TopMoverDoc']]:
        for d in cls.durations:
            for tc in cls.top_coins:
                yield [cls(query=cls.QueryDoc(duration=d, top_coins=tc), result=cls.ResultDoc(**{
                        'id': 'btc',
                        'symbol': 'btc',
                        'name': 'bitcoin',
                        'usd': 1.0,
                        'market_cap_rank': 1,
                        'usd_24h_vol': 1,
                        'usd_1y_change': 1,
                    }))]

class TopMoverCollection(ScraperCollection, doct=TopMoverDoc):
    pass

class PriceDoc(ScraperDoc):
    # https://docs.coingecko.com/v3.0.1/reference/simple-token-price

    class QueryDoc(MongoDoc):
        contract_addresses: str = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
        vs_currencies: str = 'usd'
    class ResultDoc(MongoDoc):
        usd: float

    query: QueryDoc
    result: ResultDoc

    cmd: ClassVar[str] = 'simple/token_price/ethereum'

    @classmethod
    def scrape(cls) -> Iterator[list[ScraperDoc]]:
        query = cls.QueryDoc()
        raw = list(CG(cls.cmd, **query.dict).values())[0]
        result = cls.ResultDoc(**raw)
        yield [cls(query=query, result=result)]

class PricesCollection(ScraperCollection, doct=PriceDoc):
    pass

class ScraperDB(MongoDatabase, superclass=ScraperCollection):
    top_movers: TopMoverCollection
    prices: PricesCollection

    def scrape(self):
        for name in self.fields.keys():
            getattr(self, name).scrape()

#### Yaat DB Instance ####

class YaatDBInstance(MongoInstance):
    scraper_db: ScraperDB

### Extras ####
class FrontEnd(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"whaddup yaat.club")

### Run ####

def run():
    ydb = YaatDBInstance(host='mongo' if DOCKER else None, timeoutMS=3000)

    # listeners
    web_server_job = lambda:  HTTPServer(('0.0.0.0', 80), FrontEnd).serve_forever(poll_interval=0.1)
    heart_beat_job = lambda:  make_server("0.0.0.0", 8000, lambda env, res: (res('200 OK', [('Content-type', 'text/plain; charset=utf-8')]), [b"OK"])[1]).serve_forever(poll_interval=0.1)

    # create the schedule and add the jobs. TODO - dynamic jobs
    scheduler = BlockingScheduler()
    scheduler.add_job(ydb.scraper_db.scrape, 'interval', seconds=1)
    scheduler.add_job(web_server_job, 'date', run_date=datetime.now())
    scheduler.add_job(heart_beat_job, 'date', run_date=datetime.now())

    ## start the schedule
    scheduler.start()