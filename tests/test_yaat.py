from mongoengine import connect
from tests.common import IntegrationTestCase
from yaat.scraper import TopMoverDoc, TopMoverQueryDoc
import time

class TestYaat(IntegrationTestCase, services={'yaat': False}):

    @classmethod
    def setUpClass(cls):
        connect(f"{cls.__name__}-{int(time.perf_counter())}")

    def test_simple_top_mover(self):
        q = TopMoverQueryDoc(duration='24h', top_coin='all').save()
        prev = TopMoverDoc.objects.count()
        TopMoverDoc(
            query = q,
            cid = 'fdsafdsa',
            symbol = 'btc',
            name = 'bitcoin',
            usd = 100,
            market_cap_rank = 100,
            usd_24h_vol = 100,
            usd_1y_change = 100).save()
        self.assertGreater(TopMoverDoc.objects.count(), prev)
