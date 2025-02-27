from yaat.mongo import MongoInstance
from yaat.scraper import ScraperDB, Scraper
from yaat.helpers import getenv
from apscheduler.schedulers.blocking import BlockingScheduler

DOCKER = getenv('DOCKER', False)

class YaatDBInstance(MongoInstance):
    scraper_db: ScraperDB

def run():
    ydb = YaatDBInstance(host='mongo' if DOCKER else None, timeoutMS=3000)

    scraper = Scraper({
        ydb.scraper_db.top_movers: 10**10,
        ydb.scraper_db.prices: 20**10,
    })

    scheduler = BlockingScheduler()
    scraper.add_jobs(scheduler)
    scheduler.start()