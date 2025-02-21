from pymongo import MongoClient
from yaat.yaat import Scraper, TopMoverDoc
from tests.common import IntegrationTestCase

class TestYaat(IntegrationTestCase):

    def test_TopMoversScraper(self):
        coll = Scraper(MongoClient(), TopMoverDoc).coll
        self.docker_services.wait_until_responsive(
            timeout=10.0,
            pause=0.1,
            check=lambda: (coll.count_documents({}) > 0)
        )
