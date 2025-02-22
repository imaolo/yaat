from tests.common import Doc1, Doc2, Doc1Array, Doc2Array, create_integration_test_class, Doc1Dict, Doc1EmptyIndexDoc
from yaat.mongo import MongoDoc
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

class TestMongo(create_integration_test_class()):
    pass

class TestMongoSchema(TestMongo):
    # TODO test union and optionals
    # TODO array testing

    def setUp(self):
        super().setUp()
        self.db = self.dbc['TestMongo-db']


    def test_simple_repeat_success(self):
        coll = Doc1.create_collection(self.db)
        coll = Doc1.create_collection(self.db)
        coll.insert_one(Doc1(f1='str', f2=1).dict)

    def test_simple_success(self):
        coll = Doc1.create_collection(self.db)
        coll.insert_one(Doc1(f1='str', f2=1).dict)

    def test_simple_failure_type(self):
        coll = Doc1.create_collection(self.db)
        with self.assertRaises(WriteError):
            coll.insert_one(Doc1(f1=1, f2=1).dict)

    def test_simple_failure_missing_field(self):
        coll = Doc1.create_collection(self.db)
        with self.assertRaises(WriteError):
            coll.insert_one({'f1': 'str'})

    def test_simple_failure_extra_field(self):
        coll = Doc1.create_collection(self.db)
        with self.assertRaises(WriteError):
            coll.insert_one({'f1': 'str', 'f2': 1, 'f3': 1})

    # NOTE succeeds
    def test_simple_alias_objs(self):
        coll = Doc1.create_collection(self.db)
        coll.insert_one(Doc1Alias_a(f1='str', f2=1).dict)
        coll.insert_one(Doc1Alias_b(f1='str', f2=1).dict)

    def test_nest_success(self):
        coll = Doc2.create_collection(self.db)
        coll.insert_one(Doc2(d1=Doc1(f1='str', f2=1), f1=1).dict)

    def test_nest_failure_type_prim(self):
        coll = Doc2.create_collection(self.db)
        with self.assertRaises(WriteError):
            coll.insert_one(Doc2(d1=Doc1(f1=1, f2=1), f1=1).dict)

    def test_nest_failure_type_extra_field(self):
        coll = Doc2.create_collection(self.db)
        with self.assertRaises(WriteError):
            coll.insert_one(Doc2(d1=Doc3(doc2_f1=1, f1='str', f2=1), f1=1).dict)

    def test_nest_failure_type_missing_field(self):
        coll = Doc2.create_collection(self.db)
        with self.assertRaises(WriteError):
            coll.insert_one(Doc2(d1=Doc4(f1=1), f1=1).dict)

    def test_nest_failure_type_missing_field(self):
        coll = Doc2.create_collection(self.db)
        with self.assertRaises(WriteError):
            coll.insert_one(Doc2(d1=Doc4(f1=1), f1=1).dict)

    def test_success_dict(self):
        coll = Doc1Dict.create_collection(self.db)
        coll.insert_one(Doc1Dict(dict1={}, f1=1).dict)

    def test_success_dict2(self):
        coll = Doc1Dict.create_collection(self.db)
        coll.insert_one(Doc1Dict(dict1={'1':2}, f1=1).dict)

    def test_failure_dict(self):
        coll = Doc1Dict.create_collection(self.db)
        with self.assertRaises(WriteError):
            coll.insert_one(Doc1Dict(dict1=1, f1=1).dict)

    # TODO test indexes
class TestMongoIndex(TestMongo):

    def setUp(self):
        super().setUp()
        self.db = self.dbc['TestMongo-db']

    def test_simple(self):
        pass