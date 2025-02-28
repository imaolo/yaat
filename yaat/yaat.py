from apscheduler.schedulers.blocking import BlockingScheduler
from mongoengine import connect
from datetime import datetime
from wsgiref.simple_server import make_server
from yaat.scraper import run as run_scraper, TopMoverDoc
from yaat.helpers import getenv

DOCKER = getenv('DOCKER', False)

ip = '0.0.0.0'
header = [('Content-type', 'text/plain; charset=utf-8')]
status = '200 OK'

def status_handler(_, res):
    body = f"number of top mover docs {TopMoverDoc.objects.count()}".encode()
    headers = header + [('Content-Length', str(len(body)))]
    res(status, headers)
    return [body]

def hb_handler(_, res):
    res(status, header)
    return [b"OK"]

def run():
    connect(db='yaatdb', host='mongo' if DOCKER else None, timeoutMS=3000)
    _make_listener = lambda handler, port: lambda: make_server(ip, port, handler).serve_forever(poll_interval=0.1)

    scheduler = BlockingScheduler()
    scheduler.add_job(run_scraper, 'date', run_date=datetime.now())
    scheduler.add_job(_make_listener(hb_handler, 8000), 'date', run_date=datetime.now())
    scheduler.add_job(_make_listener(status_handler, 80), 'date', run_date=datetime.now())
    scheduler.start()