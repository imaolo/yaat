from tests.common import Doc1, Doc2, Doc1Array, Doc2Array, IntegrationTestCase, Doc1Dict
from yaat.mongo import MongoCollection, MongoDoc
from pymongo.errors import WriteError

class Doc1a(Doc1):
    f99: int

class Doc3(Doc1):
    doc2_f1: int

class Doc4(MongoDoc):
    f1:int

class Doc1Alias_a(Doc1):
    pass
class Doc1Alias_b(MongoDoc):
    f1:str
    f2:int

class TestMongo(IntegrationTestCase, wait_yaat=False):
    # TODO test union and optionals
    # TODO array testing

    def setUp(self):
        super().setUp()
        self.db = self.dbc['TestMongo-db']

    def test_doc_simple_success(self):
        mcoll = MongoCollection(self.db['test_doc_success'], Doc1)
        mcoll.coll.insert_one(Doc1(f1='str', f2=1).dict)

    def test_doc_simple_failure_type(self):
        mcoll = MongoCollection(self.db['test_doc_simple_failure_type'], Doc1)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one(Doc1(f1=1, f2=1).dict)

    def test_doc_simple_failure_missing_field(self):
        mcoll = MongoCollection(self.db['test_doc_simple_failure_missing_field'], Doc1)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one({'f1': 'str'})

    def test_doc_simple_failure_extra_field(self):
        mcoll = MongoCollection(self.db['test_doc_simple_failure_missing_field'], Doc1)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one({'f1': 'str', 'f2': 1, 'f3': 1})

    # NOTE succeeds
    def test_doc_simple_alias_objs(self):
        mcoll = MongoCollection(self.db['test_doc_simple_failure_extra_field'], Doc1)
        mcoll.coll.insert_one(Doc1Alias_a(f1='str', f2=1).dict)
        mcoll.coll.insert_one(Doc1Alias_b(f1='str', f2=1).dict)

    def test_doc_nest_success(self):
        mcoll = MongoCollection(self.db['test_doc_nest_success'], Doc2)
        mcoll.coll.insert_one(Doc2(d1=Doc1(f1='str', f2=1), f1=1).dict)

    def test_doc_nest_failure_type_prim(self):
        mcoll = MongoCollection(self.db['test_doc_nest_failure_type'], Doc2)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one(Doc2(d1=Doc1(f1=1, f2=1), f1=1).dict)

    def test_doc_nest_failure_type_extra_field(self):
        mcoll = MongoCollection(self.db['test_doc_nest_failure_type_obj'], Doc2)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one(Doc2(d1=Doc3(doc2_f1=1, f1='str', f2=1), f1=1).dict)

    def test_doc_nest_failure_type_missing_field(self):
        mcoll = MongoCollection(self.db['test_doc_nest_failure_type_obj'], Doc2)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one(Doc2(d1=Doc4(f1=1), f1=1).dict)

    def test_doc_nest_failure_type_missing_field(self):
        mcoll = MongoCollection(self.db['test_doc_nest_failure_type_missing_field'], Doc2)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one(Doc2(d1=Doc4(f1=1), f1=1).dict)

    def test_doc_success_dict(self):
        mcoll = MongoCollection(self.db['test_doc_success_dict'], Doc1Dict)
        mcoll.coll.insert_one(Doc1Dict(dict1={}, f1=1).dict)

    def test_doc_success_dict2(self):
        mcoll = MongoCollection(self.db['test_doc_success_dict'], Doc1Dict)
        mcoll.coll.insert_one(Doc1Dict(dict1={'1':2}, f1=1).dict)

    def test_doc_failure_dict(self):
        mcoll = MongoCollection(self.db['test_doc_success_dict'], Doc1Dict)
        with self.assertRaises(WriteError):
            mcoll.coll.insert_one(Doc1Dict(dict1=1, f1=1).dict)