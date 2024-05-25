from yaat.util import gettime, DEBUG
from yaat.maester import Maester
from pathlib import Path
from dataclasses import asdict
from datetime import datetime
import unittest, shutil, pymongo.errors as mongoerrors

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
        ticker_dc = Maester.tickers_class('some sym', datetime.now(), 1.0, 1.0, 3.0, 4.0)
        self.maester.tickers_coll.insert_one(asdict(ticker_dc))
