from pymongo import MongoClient
from yaat.yaat import Scraper, TopMoverDoc
from yaat.helpers import wait_until_true
from tests.common import create_integration_test_class

class TestYaat(create_integration_test_class()):

    def test_TopMoversScraper(self):
        scraper = Scraper(MongoClient(), TopMoverDoc)
        wait_until_true(lambda: (scraper.coll.count_documents({}) > 0), 10, 1)
