from yaat.maester import Maester, InformerDoc
from dataclasses import asdict
from bson import Int64
import unittest, os, time, pymongo.errors as mongoerr

class TestMaester(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.m = Maester(connstr=os.getenv('CONNSTR'), dbdir=os.getenv('DBDIR'))
        cls.ts = time.time()
        cls.informer_doc = InformerDoc(root_path='somepath', data_path='somepath', target='targ', tickers=['str'],
                                       settings='settings', name=f"{cls.ts}_name", num_params=Int64(1), fields=['field'])
        # mongo uses millisecond precision
        cls.informer_doc.date = cls.informer_doc.date.replace(microsecond=int(cls.informer_doc.date.microsecond / 1000) * 1000)

    def setUp(self):
        self.informer_doc.name = f"{time.time()}_name"

    def test_informer_doc_error(self):
        with self.assertRaises(TypeError):
            InformerDoc(root_path='somepath', data_path='somepath', target='targ')

    def test_informer_doc_insert(self):
        self.m.informer_weights.insert_one(asdict(self.informer_doc))
        retdoc = InformerDoc(**self.m.informer_weights.find_one({'name': self.informer_doc.name}, {'_id': 0}))
        self.assertEqual(retdoc, self.informer_doc)

    def test_informer_doc_insert_error(self):
        self.informer_doc.tickers = [1]
        with self.assertRaises(mongoerr.WriteError):
            self.m.informer_weights.insert_one(asdict(self.informer_doc))
