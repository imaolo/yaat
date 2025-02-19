from yaat.util import gettime, rm, DEBUG
from yaat.miner import Miner, Depo
import unittest

def getid(tc:unittest.TestCase): return tc.id().split('.')[-1]
class TestMinerSetup(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.depo = Depo(db_name=str(gettime())+"_db", dir=f"twork/twork_{cls.__name__}_{gettime()}", connstr=None)
    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: rm('twork')

class TestDepo(TestMinerSetup):

    def test_def_constructor(self):
        self.depo.db.command('ping')
        for pc in ['stocks', 'currs']:
            ticn =  pc+'_tickers'
            mdn =  pc+'_metadata'
            self.assertTrue(ticn in self.depo.db.list_collection_names())
            self.assertTrue('datetime_1' in self.depo.db[ticn].index_information())
            self.assertTrue(mdn in self.depo.db.list_collection_names())
            self.assertTrue('metadata_1' in self.depo.db[mdn].index_information())

    def test_create_colls(self):
        cn = 'test_create_colls_coll'
        ticn = cn + '_tickers'
        mdn =  cn + '_metadata'
        self.depo.create_colls(cn)
        self.assertTrue(ticn in self.depo.db.list_collection_names())
        self.assertTrue('datetime_1' in self.depo.db[ticn].index_information())
        self.assertTrue(mdn in self.depo.db.list_collection_names())
        self.assertTrue('metadata_1' in self.depo.db[mdn].index_information())

    def test_insert_metadata_simple(self):
        self.depo.stocks_metadata.delete_many({})
        rid = self.depo.insert_metadata(self.depo.stocks_metadata, {'Meta Data': ''})
        self.assertTrue(self.depo.stocks_metadata.count_documents({}) == 1)

    def test_insert_metadata_no_meta_data_key(self):
        with self.assertRaises(KeyError): self.depo.insert_metadata(self.depo.stocks_metadata, {})
class TestMiner(unittest.TestCase):
    def setUp(self): self.miner = Miner()

    def test_mine_simple(self): pass