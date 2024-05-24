from yaat.util import gettime, DEBUG
from yaat.maester import Maester
from pathlib import Path
import unittest, shutil

def getid(tc:unittest.TestCase): return tc.id().split('.')[-1]

class TestMaester(unittest.TestCase):

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