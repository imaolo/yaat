from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
from mongoengine import connect
from datetime import datetime
from yaat.scraper import run as run_scraper
from yaat.helpers import getenv
from yaat.webapp import WEBAPP
import threading, time, sys

def run():
    # create the schedule
    scheduler = BackgroundScheduler()

    # add the jobs    
    scheduler.add_job(WEBAPP.run, 'date', run_date=datetime.now())
    scheduler.add_job(run_scraper, 'date', run_date=datetime.now())

    # exit listener
    shutdown_event = threading.Event()
    def exit(event):
        if event.exception:
            shutdown_event.set()
            WEBAPP.should_exit = True
            scheduler.shutdown()
    scheduler.add_listener(exit, EVENT_JOB_ERROR)

    # connect to the database and start the app
    connect(db='yaatdb', host='mongo' if getenv('DOCKER', False) else None, timeoutMS=3000)
    scheduler.start()

    # wait for exit
    while not shutdown_event.is_set(): time.sleep(0.1)
    sys.exit(1)