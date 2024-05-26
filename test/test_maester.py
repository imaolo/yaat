from yaat.util import gettime, killproc, DEBUG
from yaat.maester import Maester
from pathlib import Path
from dataclasses import asdict
from datetime import datetime
from zoneinfo import ZoneInfo
import unittest, shutil, atexit, functools, pymongo.errors as mongoerrors

def getid(tc:unittest.TestCase): return tc.id().split('.')[-1]

class TestMaesterConstructDelete(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = Path(f"twork/twork_{cls.__name__}_{gettime()}")
        cls.dp.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: shutil.rmtree(cls.dp)

    def test_constructor_bad_args(self):
        with self.assertRaises(AssertionError): Maester('some conn str', 'some db dir')

    def test_constructor_dbdir_path(self):
        m = Maester(None, str(self.dp / (getid(self) + '_db')))
        self.assertTrue(isinstance(m.dbdir, Path))
        del m

    def test_cleanup(self):
        m = Maester(None, str(self.dp / (getid(self) + '_db')))
        proc = m.mongo_proc
        del m
        self.assertEqual(proc.poll(), 0)

    def test_2_dbs(self):
        m = Maester(None, str(self.dp / (getid(self) + '_db1')))
        with self.assertRaises(AssertionError):  Maester(None, str(self.dp / (getid(self) + '_db2')))
    
    def test_connstr(self):
        _, proc = Maester.startlocdb(self.dp / (getid(self) + '_db'))
        atexit.register(functools.partial(killproc, proc))
        m = Maester()
        del m
        killproc(proc)

    def test_business_hours(self):
        self.assertFalse(Maester.is_business_hours(datetime(2024, 7, 1)))
        self.assertFalse(Maester.is_business_hours(datetime(2024, 7, 2, 8, tzinfo=ZoneInfo('US/Eastern'))))
        self.assertFalse(Maester.is_business_hours(datetime(2024, 7, 2, 9, 29, tzinfo=ZoneInfo('US/Eastern'))))
        self.assertTrue(Maester.is_business_hours(datetime(2024, 7, 2, 9, 30, tzinfo=ZoneInfo('US/Eastern'))))
        self.assertTrue(Maester.is_business_hours(datetime(2024, 7, 2, 15, 30, tzinfo=ZoneInfo('US/Eastern'))))
        self.assertFalse(Maester.is_business_hours(datetime(2024, 7, 2, 16, 31, tzinfo=ZoneInfo('US/Eastern'))))

class TestMaesterTickersColl(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = Path(f"twork/twork_{cls.__name__}_{gettime()}")
        cls.dp.mkdir(parents=True, exist_ok=True)
        cls.maester = Maester(None, cls.dp / 'dbdir')

    @classmethod
    def tearDownClass(cls) -> None:
        del cls.maester
        if not DEBUG: shutil.rmtree(cls.dp)

    def test_schema_bad_keys(self):
        with self.assertRaises(mongoerrors.WriteError): self.maester.tickers_coll.insert_one({'dummy': 'doc'})

    def test_tickers_schema_and_dataclass_agree(self):
        ticker_dc = Maester.ticker_class('some sym', datetime.now(), 1.0, 1.0, 3.0, 4.0)
        self.maester.tickers_coll.insert_one(asdict(ticker_dc))

    def test_duplicate_insert(self):
        ticker_dc = Maester.ticker_class('some sym', datetime.now(), 1.0, 1.0, 3.0, 4.0)
        self.maester.tickers_coll.insert_one(asdict(ticker_dc))
        with self.assertRaises(mongoerrors.DuplicateKeyError): self.maester.tickers_coll.insert_one(asdict(ticker_dc))