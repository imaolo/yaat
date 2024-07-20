from yaat.maester import Maester, InformerDoc
from dataclasses import asdict
from bson import Int64
import unittest, os, time

class TestMaester(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.m = Maester(connstr=os.getenv('CONNSTR'), dbdir=os.getenv('DBDIR'))

    def test_informer_doc_error(self):
        with self.assertRaises(TypeError):
            InformerDoc(root_path='somepath', data_path='somepath', target='targ')

    def test_informer_doc_insert(self):
        doc = InformerDoc(root_path='somepath', data_path='somepath', target='targ', tickers=['str'],
                           settings='settings', name=f"{time.time()}_name", num_params=Int64(1), fields=['field'])
        self.m.informer_weights.insert_one(asdict(doc))
