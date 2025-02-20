from tests.common import Doc1, Doc2, Doc1Array, Doc2Array, IntegrationTestCase
from yaat.mongo import MongoCollection
from pymongo.errors import WriteError

class Doc1a(Doc1):
    f99: int

class TestMongo(IntegrationTestCase, wait_yaat=False):
    # TODO test union and optionals

    def setUp(self):
        super().setUp()
        self.db = self.dbc['TestMongo-db']

    def test_doc_success(self):
        mcoll = MongoCollection(self.db['test_doc_success'], Doc1)
        mcoll.coll.insert_one(Doc1(f1='str', f2=1).asdict())

    def test_doc_failure_type(self):
        mcoll = MongoCollection(self.db['test_doc_simple_failure_type'], Doc1)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one(Doc1(f1=1, f2=1).asdict())

    def test_doc_failure_missing_field(self):
        mcoll = MongoCollection(self.db['test_doc_simple_failure_missing_field'], Doc1)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one({'f1': 'str'})

    def test_doc_failure_extra_field(self):
        mcoll = MongoCollection(self.db['test_doc_simple_failure_extra_field'], Doc1)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one({'f1': 'str', 'f2': 1, 'fdsafsd': 1})