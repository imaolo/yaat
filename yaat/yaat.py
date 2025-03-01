from apscheduler.schedulers.blocking import BlockingScheduler
from mongoengine import connect
from datetime import datetime
from wsgiref.simple_server import make_server
from yaat.helpers import getenv
from yaat.webapp import run as run_webapp

DOCKER = getenv('DOCKER', False)

header = [('Content-type', 'text/plain; charset=utf-8')]
def hb_handler(_, res):
    body = b"OK"
    headers = header + [('Content-Length', str(len(body)))]
    res('200 OK', headers)
    return [body]

def run_hb():
    make_server('0.0.0.0', 8000, hb_handler).serve_forever(poll_interval=0.1)

def run():
    connect(db='yaatdb', host='mongo' if DOCKER else None, timeoutMS=3000)

    scheduler = BlockingScheduler()

    scheduler.add_job(run_hb, 'date', run_date=datetime.now())
    scheduler.add_job(run_webapp, 'date', run_date=datetime.now())
    scheduler.add_job(run_scraper, 'date', run_date=datetime.now())

    scheduler.start()