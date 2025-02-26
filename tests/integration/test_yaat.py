from yaat.yaat import YaatDBInstance
from yaat.helpers import wait_until_true
import unittest

class TestYaat(unittest.TestCase):
    def test_TopMoversScraper(self):
        coll = YaatDBInstance().scraper_db.top_movers
        wait_until_true(lambda: (coll.count_documents({}) > 0), 10, 1)
