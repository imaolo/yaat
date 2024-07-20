from yaat.maester import Maester, InformerDoc
from dataclasses import asdict
from bson import Int64
import unittest, os, time

class TestMaester(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.m = Maester(connstr=os.getenv('CONNSTR'), dbdir=os.getenv('DBDIR'))
        cls.ts = time.time()
        cls.informer_doc = InformerDoc(root_path='somepath', data_path='somepath', target='targ', tickers=['str'],
                                       settings='settings', name=f"{cls.ts}_name", num_params=Int64(1), fields=['field'])
        # mongo uses millisecond precision
        cls.informer_doc.date = cls.informer_doc.date.replace(microsecond=int(cls.informer_doc.date.microsecond / 1000) * 1000)

    def test_informer_doc_error(self):
        with self.assertRaises(TypeError):
            InformerDoc(root_path='somepath', data_path='somepath', target='targ')

    def test_informer_doc_insert(self):
        self.m.informer_weights.insert_one(asdict(self.informer_doc))
        doc = self.m.informer_weights.find_one({'name': self.informer_doc.name}, {'_id': 0})
        retdoc = InformerDoc(**self.m.informer_weights.find_one({'name': self.informer_doc.name}, {'_id': 0}))
        self.assertEqual(retdoc, self.informer_doc)

