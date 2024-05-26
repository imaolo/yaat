from yaat.util import gettime, DEBUG
from yaat.miner import Miner
from yaat.maester import Maester
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
import unittest, shutil

def getid(tc:unittest.TestCase): return tc.id().split('.')[-1]

class TestMiner(unittest.TestCase):

    end:datetime = datetime.combine(datetime.now().date(), datetime.min.time())
    start:datetime = end - timedelta(weeks=12)
    middle:datetime = datetime.combine((start + ((end - start) / 2)).date(), datetime.min.time())
    sym:str = 'SPY'
    sat = datetime(2023, 4, 1, tzinfo=ZoneInfo('US/Eastern'))
    sun = datetime(2023, 4, 2, tzinfo=ZoneInfo('US/Eastern'))
    mon = datetime(2023, 4, 3, tzinfo=ZoneInfo('US/Eastern'))
    tue = datetime(2023, 4, 4, tzinfo=ZoneInfo('US/Eastern'))
    wed = datetime(2023, 4, 5, tzinfo=ZoneInfo('US/Eastern'))

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = Path(f"twork/twork_{cls.__name__}_{gettime()}")
        cls.dp.mkdir(parents=True, exist_ok=True)
        cls.miner = Miner(Maester(connstr=None, dbdir=cls.dp/'testdb'))

    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: shutil.rmtree(cls.dp)

    def setUp(self) -> None:
        self.clean_tickers_coll()
        return super().setUp()

    def tearDown(self) -> None:
        self.clean_tickers_coll()
        return super().tearDown()

    # helpers

    def clean_tickers_coll(self): self.miner.maester.tickers_coll.delete_many({})

    def create_dt_ticker(self, dt: datetime=middle) -> Maester.ticker_class: return Maester.ticker_class(self.sym, dt, *[1.0]*4)

    # tests

    def test_get_int_syms_combo(self):
        # the plus 1s are because the date ranges are inclusive

        combos = self.miner.get_int_sym_combos(60, datetime(2021, 1, 1), datetime(2021, 1, 2), [self.sym], False)
        self.assertEqual(len(combos), 24+1)

        combos = self.miner.get_int_sym_combos(60, datetime(2021, 1, 1), datetime(2021, 1, 2), [self.sym]*2, False)
        self.assertEqual(len(combos), (24+1)*2)

        combos = self.miner.get_int_sym_combos(5, datetime(2021, 1, 1, 1), datetime(2021, 1, 1, 2), [self.sym], False)
        self.assertEqual(len(combos), 12+1)

    def test_get_missing_tickers(self):
        freq = 5
        self.miner.maester.insert_ticker(self.create_dt_ticker())
        combos = self.miner.get_int_sym_combos(freq, self.start, self.end, [self.sym], False)
        missing_ticks = self.miner.get_missing_tickers(freq, self.start, self.end, [self.sym], False)
        self.assertEqual(len(combos)-1, len(missing_ticks))

    def test_mine_alpha_simple_day_60m(self):
        freq = 60
        ints = self.miner.get_intervals(freq, self.mon, self.tue, True)
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.tue, [self.sym]))
        self.assertEqual(len(inserted), len(ints)-1) # the api is not inclusive, but we aren
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.tue, [self.sym]))
        self.assertEqual(len(inserted), 0)

    def test_mine_alpha_simple_day_30m(self):
        freq = 30
        ints = self.miner.get_intervals(freq, self.mon, self.tue, True)
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.tue, [self.sym]))
        self.assertEqual(len(inserted), len(ints)-1) # the api is not inclusive, but we are
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.tue, [self.sym]))
        self.assertEqual(len(inserted), 0)

    def test_mine_alpha_crack_day_60m(self):
        freq = 60
        ints = self.miner.get_intervals(freq, self.mon, self.tue, True)
        self.miner.maester.insert_ticker(self.create_dt_ticker(self.mon.replace(hour=10)))
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.tue, [self.sym]))
        self.assertEqual(len(inserted), len(ints)-1-1) # added one document
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.tue, [self.sym]))
        self.assertEqual(len(inserted), 0) # no all filled

    def test_mine_alpha_cracks_day_15m(self):
        freq = 15
        ints = self.miner.get_intervals(freq, self.mon, self.tue, True)
        self.miner.maester.insert_ticker(self.create_dt_ticker(self.mon.replace(hour=10)))
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.tue, [self.sym]))
        self.assertEqual(len(inserted), len(ints)-1-1) # added one document
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.tue, [self.sym]))
        self.assertEqual(len(inserted), 0)

    def test_mine_alpha_cracks_day_60m_15m(self):
        ins60 = list(self.miner.mine_alpha(60, self.mon, self.tue, [self.sym])) # 10, 11, 12, 1, 2, 3
        ins30 = list(self.miner.mine_alpha(30, self.mon, self.tue, [self.sym])) # 930, 1030, 1130, 1230, 130, 230, 330
        self.assertEqual(len(ins60), 6)
        self.assertEqual(len(ins30), 7)

    def test_mine_alpha_simple_2day_60m(self):
        freq = 60
        ints = self.miner.get_intervals(freq, self.mon, self.wed, True)
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.wed, [self.sym]))
        self.assertEqual(len(inserted), len(ints)-2) # the api is not inclusive, but we are
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.wed, [self.sym]))
        self.assertEqual(len(inserted), 0)
        inserted = list(self.miner.mine_alpha(freq, self.mon, self.tue, [self.sym]))
        self.assertEqual(len(inserted), 0)
