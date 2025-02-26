from tests.common import Doc1, Doc2, Doc1Dict, IntegrationTestCase
from yaat.mongo import MongoDoc, CollDoc, IndexDoc, MongoCollection
from pymongo.errors import WriteError, OperationFailure
from pymongo import MongoClient
import time, abc

class Doc1a(Doc1):
    f99: int

class Doc3(Doc1):
    doc2_f1: int

class Doc4(MongoDoc):
    f1:int

test_mongo_services = {'yaat': False}
class TestMongo(IntegrationTestCase, abc.ABC, services=test_mongo_services):
    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(services=test_mongo_services, *args, **kwargs)

    _db = None

    @property
    def db(self) -> MongoClient:
        if self._db is None: self._db = MongoClient(host=self.docker_ip)[f"{type(self).__name__}-{int(time.perf_counter())}"]
        return self._db

    @property
    def test_name(self) -> str: return self.id().split('.')[-1]

class TestMongoSchema(TestMongo):
    # TODO test union and optionals
    # TODO array/list testing

    def test_simple_insert_success(self):
        collt = MongoCollection[Doc1]
        collt(self.db, self.test_name).insert_one(collt.doct(f1='str', f2=1).dict)

    def test_simple_insert_success(self):
        MongoCollection[Doc1](self.db, self.test_name).insert_one(Doc1(f1='str', f2=1).dict)

    def test_simple_failure_type(self):
        with self.assertRaises(WriteError):
            MongoCollection[Doc1](self.db, self.test_name).insert_one(Doc1(f1=1, f2=1).dict)

    def test_simple_failure_missing_field(self):
        with self.assertRaises(WriteError):
            MongoCollection[Doc1](self.db, self.test_name).insert_one({'f1': 'str'})

    def test_simple_failure_extra_field(self):
        with self.assertRaises(WriteError):
            MongoCollection[Doc1](self.db, self.test_name).insert_one({'f1': 'str', 'f2': 1, 'f3': 1})

    def test_nest_success(self):
        MongoCollection[Doc2](self.db, self.test_name).insert_one(Doc2(d1=Doc1(f1='str', f2=1), f1=1).dict)

    def test_nest_failure_type_prim(self):
        with self.assertRaises(WriteError):
            MongoCollection[Doc2](self.db, self.test_name).insert_one(Doc2(d1=Doc1(f1=1, f2=1), f1=1).dict)

    def test_nest_failure_type_extra_field(self):
        with self.assertRaises(WriteError):
            MongoCollection[Doc2](self.db, self.test_name).insert_one(Doc2(d1=Doc3(doc2_f1=1, f1='str', f2=1), f1=1).dict)

    def test_nest_failure_type_missing_field(self):
        with self.assertRaises(WriteError):
            MongoCollection[Doc2](self.db, self.test_name).insert_one(Doc2(d1=Doc4(f1=1), f1=1).dict)

    def test_nest_failure_type_missing_field(self):
        with self.assertRaises(WriteError):
            MongoCollection[Doc2](self.db, self.test_name).insert_one(Doc2(d1=Doc4(f1=1), f1=1).dict)

    def test_success_dict(self):
        MongoCollection[Doc1Dict](self.db, self.test_name).insert_one(Doc1Dict(dict1={}, f1=1).dict)

    def test_success_dict2(self):
        MongoCollection[Doc1Dict](self.db, self.test_name).insert_one(Doc1Dict(dict1={'1':2}, f1=1).dict)

    def test_failure_dict(self):
        with self.assertRaises(WriteError):
            MongoCollection[Doc1Dict](self.db, self.test_name).insert_one(Doc1Dict(dict1=1, f1=1).dict)

class TestMongoIndex(TestMongo):

    def test_simple_none_success(self):
        coll = MongoCollection[Doc1](self.db, self.test_name)
        self.assertEqual(first=(idxinfo:=coll.index_information()), msg=idxinfo, second={
            '_id_': {
                'v': 2,
                'key': [('_id', 1)]}})

    def test_simple_single_default_success(self):
        class Doc1SingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc.create('new_f1'))):
            new_f1 :int # NOTE Needed because docs cannot have the same fields
        coll = MongoCollection[Doc1SingleDefaultIndex](self.db, self.test_name)
        self.assertEqual(first=(idxinfo:=coll.index_information()), msg=idxinfo, second={
            '_id_': {
                'key': [('_id', 1)],
                'v': 2},
            'new_f1_1': {
                'key': [('new_f1', 1)],
                'v': 2}})

    def test_simple_single_unique_success(self):
        class Doc1SingleUniqueIndex(Doc1, colldoc=CollDoc(index=IndexDoc.create('new_f4', unique=True))):
            new_f4 :int # NOTE Needed because docs cannot have the same fields
        coll = MongoCollection[Doc1SingleUniqueIndex](self.db, self.test_name)
        self.assertEqual(first=(idxinfo:=coll.index_information()), msg=idxinfo, second={
            '_id_': {
                'key': [('_id', 1)],
                'v': 2},
            'new_f4_1': {
                'key': [('new_f4', 1)],
                'v': 2,
                'unique': True}})

    def test_new_options_failure(self):
        class Doc1SingleDefaultIndexRepeat(Doc1, colldoc=CollDoc(index=IndexDoc.create('new_f1'))):
            new_f2 :int # NOTE Needed because docs cannot have the same fields
        MongoCollection[Doc1SingleDefaultIndexRepeat](self.db, self.test_name)
        Doc1SingleDefaultIndexRepeat.colldoc = CollDoc(index=IndexDoc.create('new_f1', unique=True))
        with self.assertRaises(OperationFailure):
            MongoCollection[Doc1SingleDefaultIndexRepeat](self.db, self.test_name)


# ## Graveyard
#     def test_simple_unique_success(self):
#         pass
#         coll = self.dbc['db'][f'mycoll{time.perf_counter()//1}']
#         print()
#         print(coll.index_information(), end='\n')
#         coll.create_index('f1')
#         print(coll.index_information(), end='\n')
#         class Doc1SingleDefaultUniqueIndex(Doc1, colldoc=CollDoc(index=IndexDoc.create(['f1'], unique=True))):
#             pass
#         coll = MongoCollection[Doc1SingleDefaultUniqueIndex](self.db, self.test_name)
#         self.assertEqual(first=(idxinfo:=coll.index_information()), msg=idxinfo, second={
#             '_id_': {
#                 'v': 2,
#                 'key': [('_id', 1)]},
#             'f1_1_f2_1': {
#                 'v': 2,
#                 'key': [('f1', 1), ('f2', 1)],
#                 'unique': True},
#             'f1_1': {
#                 'v': 2,
#                 'key': [('f1', 1)]}})


#     def test_simple_empty_failure(self):
#         class Doc1EmptyIndex(Doc1, colldoc=CollDoc(index=IndexDoc.create())):
#             pass
#         with self.assertRaises(TypeError):
#             MongoCollection[Doc1EmptyIndex](self.db, self.test_name)

#     def test_simple_default_single_success(self):
#         class Doc1SingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc(args=['f1'], kwargs={}))):
#             pass
#         MongoCollection[Doc1SingleDefaultIndex](self.db, self.test_name)
#         MongoCollection[Doc1SingleDefaultIndex](self.db, self.test_name)
#         MongoCollection[Doc1SingleDefaultIndex](self.db, self.test_name)

#     def test_simple_default_single_repeat_failure(self):
#         class Doc1aSingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc(args=['f1'], kwargs={}))):
#             pass
#         MongoCollection[Doc1aSingleDefaultIndex](self.db, self.test_name)
#         # with self.assertRaises(OperationFailure):
#         #     MongoCollection[Doc1aSingleDefaultIndex](self.db, self.test_name)

#     def test_simple_default_compound(self):
#         class Doc1SingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc.create(('f1', 'f2'), unique=True))):
#             pass
#         MongoCollection[Doc1SingleDefaultIndex](self.db, self.test_name)
#         MongoCollection[Doc1SingleDefaultIndex](self.db, self.test_name)
