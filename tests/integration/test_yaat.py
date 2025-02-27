from yaat.yaat import YaatDBInstance
from yaat.scraper import ScraperCollection
from tests.common import IntegrationTestCase

class TestYaat(IntegrationTestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbi = YaatDBInstance()

    def helper_test_scraper_coll(self, coll: ScraperCollection):
        prev_docs = coll.count_documents({})
        coll.scrape()
        self.assertGreater(coll.count_documents({}), prev_docs)

    def test_TopMoversScraper(self):
        self.helper_test_scraper_coll(self.dbi.scraper_db.top_movers)

    def test_PricesScraper(self):
        self.helper_test_scraper_coll(self.dbi.scraper_db.prices)
