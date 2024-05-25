from yaat.util import gettime, DEBUG
from yaat.miner import Miner
from yaat.maester import Maester
from pathlib import Path
from datetime import datetime
import unittest, shutil

def getid(tc:unittest.TestCase): return tc.id().split('.')[-1]

class TestMiner(unittest.TestCase):

    start = datetime(1,1,1)
    end = datetime.now()
    sym = 'test_sym'

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

    def create_dt_ticker(self, dt: datetime) -> Maester.ticker_class: return Maester.ticker_class(self.sym, dt, *[1.0]*4)

    # tests
    
    def test_valid_freq(self):
        for freq in (1, 5, 15, 30, 60): self.miner.check_freq_min(freq)
        with self.assertRaises(AssertionError): self.miner.check_freq_min(7)

    def test_get_existing_tickers_freq_1m(self):
        self.miner.insert_ticker(self.create_dt_ticker(datetime(2001, 1, 1, 1, 1)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(5, self.start, self.end, [self.sym])), 0)

    def test_get_existing_tickers_freq_5m(self):
        self.miner.insert_ticker(self.create_dt_ticker(datetime(2001, 1, 1, 1, 35)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(5, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(15, self.start, self.end, [self.sym])), 0)

    def test_get_existing_tickers_freq_15m(self):
        self.miner.insert_ticker(self.create_dt_ticker(datetime(2001, 1, 1, 1, 45)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(5, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(15, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(30, self.start, self.end, [self.sym])), 0)

    def test_get_existing_tickers_freq_30m(self):
        self.miner.insert_ticker(self.create_dt_ticker(datetime(2001, 1, 1, 1, 30)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(5, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(15, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(30, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(60, self.start, self.end, [self.sym])), 0)

    def test_get_existing_tickers_freq_60m(self):
        self.miner.insert_ticker(self.create_dt_ticker(datetime(2001, 1, 1, 1)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(5, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(15, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(30, self.start, self.end, [self.sym])), 1)
        self.assertEqual(len(self.miner.get_existing_tickers(60, self.start, self.end, [self.sym])), 1)

    def test_get_existing_tickers_nonzero_sec(self):
        self.miner.insert_ticker(self.create_dt_ticker(datetime(2001, 1, 1, 1, 1, 1)))
        self.assertEqual(len(self.miner.get_existing_tickers(1, self.start, self.end, [self.sym])), 0)

    def test_get_missing_tickers(self):
        start = datetime(2021, 1, 1)
        end = datetime(2021, 1, 2)
        diff = end - start
        minutes = diff.total_seconds()/60
        missing_ticks = self.miner.get_missing_tickers(1, start, end, ['some sym'])
        self.assertEqual(len(missing_ticks), int(minutes + 1)) # plus 1 for current time bucket