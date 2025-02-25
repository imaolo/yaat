from yaat.mongo import MongoDoc, MongoCollection, MongoDatabase, MongoInstance
from pymongo import MongoClient
from apscheduler.schedulers.blocking import BlockingScheduler
from abc import ABC, abstractmethod
from datetime import datetime
from dataclasses import field
from wsgiref.simple_server import make_server
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Generator, ClassVar

## job definitions

class TopMoverDoc(MongoDoc):
    # https://docs.coingecko.com/reference/coins-top-gainers-losers

    # sub document definitions
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

    # document fields
    query: QueryDoc
    result: ResultDoc
    timestamp: datetime = field(default_factory=datetime.now)

    # class information
    durations: ClassVar[list[str]] = ['1h', '24h', '7d', '14d', '30d', '1y']
    top_coins: ClassVar[list[str]] = ['300', '500', '1000', 'all']

    @classmethod
    def retrieve(cls) -> Generator[list[MongoDoc], None, None]:
        for duration in cls.durations:
            for top_coin in cls.top_coins:
                yield [cls(query=cls.QueryDoc(duration=duration, top_coins=top_coin), result=cls.ResultDoc(**{
                        'id': 'btc',
                        'symbol': 'btc',
                        'name': 'bitcoin',
                        'usd': 1.0,
                        'market_cap_rank': 1,
                        'usd_24h_vol': 1,
                        'usd_1y_change': 1,
                    }))]
class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"whaddup yaat.club")


class ScraperDB(MongoDatabase):
    top_movers: MongoCollection[TopMoverDoc]

class YaatDBInstance(MongoInstance):
    scraper_db: ScraperDB

## start yaat python component

def run():
    top_movers_doc = YaatDBInstance().scraper_db.top_movers.doc

    # create the jobs
    web_server_job = lambda:  HTTPServer(('0.0.0.0', 80), SimpleHandler).serve_forever(poll_interval=0.1)
    heart_beat_job = lambda:  make_server("0.0.0.0", 8000, lambda env, res: (res('200 OK', [('Content-type', 'text/plain; charset=utf-8')]), [b"OK"])[1]).serve_forever(poll_interval=0.1)

    ## create the schedule and add the jobs. TODO - dynamic jobs
    scheduler = BlockingScheduler()
    scheduler.add_job(lambda: top_movers_doc.retrieve, 'interval', seconds=1)
    scheduler.add_job(web_server_job, 'date', run_date=datetime.now())
    scheduler.add_job(heart_beat_job, 'date', run_date=datetime.now())

    ## start the schedule
    scheduler.start()
