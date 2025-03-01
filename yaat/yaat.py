from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_ERROR
from mongoengine import connect
from datetime import datetime
from yaat.scraper import run as run_scraper
from yaat.webapp import run as run_webapp
from yaat.helpers import getenv
import os, signal

def run():
    # create the schedule
    scheduler = BlockingScheduler()

    # add the jobs    
    scheduler.add_job(run_webapp, 'date', run_date=datetime.now())
    scheduler.add_job(run_scraper, 'date', run_date=datetime.now())

    # exit listener
    def exit(event):
        if event.exception:
            os.kill(os.getpid(), signal.SIGTERM)
    scheduler.add_listener(exit, EVENT_JOB_ERROR)

    # connect to the database and start the schedule
    connect(db='yaatdb', host='mongo' if getenv('DOCKER', False) else None, timeoutMS=3000)
    scheduler.start()