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
    
    def test_valid_freq(self):
        for freq in (1, 5, 15, 30, 60): self.miner.check_freq_min(freq)
        with self.assertRaises(AssertionError): self.miner.check_freq_min(7)

    def test_get_existing_tickers_freq_1m(self):
        self.miner.insert_ticker(self.create_dt_ticker(self.middle.replace(minute=1)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(5, self.start, self.end, [self.sym])), 0)

    def test_get_existing_tickers_freq_5m(self):
        self.miner.insert_ticker(self.create_dt_ticker(self.middle.replace(minute=35)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(5, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(15, self.start, self.end, [self.sym])), 0)

    def test_get_existing_tickers_freq_15m(self):
        self.miner.insert_ticker(self.create_dt_ticker(self.middle.replace(minute=45)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(5, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(15, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(30, self.start, self.end, [self.sym])), 0)

    def test_get_existing_tickers_freq_30m(self):
        self.miner.insert_ticker(self.create_dt_ticker(self.middle.replace(minute=30)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(5, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(15, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(30, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(60, self.start, self.end, [self.sym])), 0)

    def test_get_existing_tickers_freq_60m(self):
        self.miner.insert_ticker(self.create_dt_ticker(self.middle))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(5, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(15, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(30, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(60, self.start, self.end, [self.sym])), 1)

    def test_get_existing_tickers_nonzero_sec(self):
        self.miner.insert_ticker(self.create_dt_ticker(self.middle.replace(second=1)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 0)

    def test_get_intervals(self):
        sat = datetime(2024, 6, 1, tzinfo=ZoneInfo('US/Eastern'))
        sun = datetime(2024, 6, 2, tzinfo=ZoneInfo('US/Eastern'))
        mon = datetime(2024, 6, 3, tzinfo=ZoneInfo('US/Eastern'))
        tue = datetime(2024, 6, 4, tzinfo=ZoneInfo('US/Eastern'))
        self.assertEqual(len(self.miner.get_intervals(60, sat, sun, True)), 0)
        self.assertEqual(len(self.miner.get_intervals(30, sat, sun, False)), 48+1)
        self.assertEqual(len(self.miner.get_intervals(60, sun, mon, True)), 0)
        self.assertEqual(len(self.miner.get_intervals(60, mon, tue, True)), 7)
        self.assertEqual(len(self.miner.get_intervals(30, mon, tue, True)), 14)


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
        self.miner.insert_ticker(self.create_dt_ticker())
        combos = self.miner.get_int_sym_combos(freq, self.start, self.end, [self.sym], False)
        missing_ticks = self.miner.get_missing_tickers(freq, self.start, self.end, [self.sym], False)
        self.assertEqual(len(combos)-1, len(missing_ticks))

    # def test_mine_alpha(self):
    #     freq = 60
    #     start = datetime(2021, 1, 4)
    #     end = datetime(2021, 1, 5)
    #     inserted = self.miner.mine_alpha(freq, start, end, [self.sym])
    #     myprint('?', inserted)
    #     self.assertEqual(len(inserted), 25)