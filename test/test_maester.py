from yaat.informer import Informer, InformerArgs
from yaat.maester import Maester
import unittest, os

class TestMaester(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.m = Maester(connstr=os.getenv('CONNSTR'), dbdir=os.getenv('DBDIR'))

    def test_void(self): pass
