from yaat.mongo import MongoDoc
from pymongo import MongoClient
from apscheduler.schedulers.blocking import BlockingScheduler
from abc import ABC, abstractmethod
from datetime import datetime
from dataclasses import field
from wsgiref.simple_server import make_server
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Generator, ClassVar

## utils

class RetrieverDoc(MongoDoc, ABC):
    @abstractmethod
    def retrieve(cls) -> Generator[list[MongoDoc], None, None]:
        pass

class Scraper:
    def __init__(self, dbc: MongoClient, retriever_doc: type[RetrieverDoc]):
        self.retriever_doc = retriever_doc
        self.coll = self.retriever_doc.create_collection(dbc[type(self).__name__])

    def __call__(self):
        for docs in self.retriever_doc.retrieve():
            self.coll.insert_many([d.dict for d in docs])

## job definitions

class TopMoverDoc(RetrieverDoc):
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

def run():
    dbc = MongoClient(host='mongo')

    # create the jobs
    top_mover_scraper_job = Scraper(dbc, TopMoverDoc)
    web_server_job = lambda:  HTTPServer(('0.0.0.0', 80), SimpleHandler).serve_forever(poll_interval=0.1)
    heart_beat_job = lambda:  make_server("0.0.0.0", 8000, lambda env, res: (res('200 OK', [('Content-type', 'text/plain; charset=utf-8')]), [b"OK"])[1]).serve_forever(poll_interval=0.1)

    ## create the schedule and add the jobs. TODO - dynamic jobs
    scheduler = BlockingScheduler()
    scheduler.add_job(top_mover_scraper_job, 'interval', seconds=1)
    scheduler.add_job(web_server_job, 'date', run_date=datetime.now())
    scheduler.add_job(heart_beat_job, 'date', run_date=datetime.now())

    ## start the schedule
    scheduler.start()
