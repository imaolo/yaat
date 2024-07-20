from yaat.maester import Maester, InformerDoc
import unittest, os

class TestMaester(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.m = Maester(connstr=os.getenv('CONNSTR'), dbdir=os.getenv('DBDIR'))

    def test_informer_doc_error(self):
        with self.assertRaises(TypeError):
            InformerDoc(root_path='somepath', data_path='somepath', target='targ')
