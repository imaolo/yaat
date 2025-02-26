from yaat.yaat import YaatDBInstance
from yaat.helpers import wait_until_true
from tests.common import IntegrationTestCase

class TestYaat(IntegrationTestCase):
    def test_TopMoversScraper(self):
        coll = YaatDBInstance().scraper_db.top_movers
        wait_until_true(lambda: (coll.count_documents({}) > 0), 10, 1)

    def test_PricesScraper(self):
        coll = YaatDBInstance().scraper_db.prices
        wait_until_true(lambda: (coll.count_documents({}) > 0), 10, 1)
