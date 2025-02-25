from tests.common import Doc1, Doc2, Doc1Dict, create_integration_test_class
from yaat.mongo import MongoDoc, CollDoc, IndexDoc, MongoCollection
from pymongo.errors import WriteError, OperationFailure
import time

class Doc1a(Doc1):
    f99: int

class Doc3(Doc1):
    doc2_f1: int

class Doc4(MongoDoc):
    f1:int
class TestMongo(create_integration_test_class()):
    def setUp(self):
        super().setUp()
        self.db = self.dbc[f"{type(self).__name__}-{int(time.perf_counter())}"]

    @property
    def test_name(self) -> str: return self.id().split('.')[-1]
class TestMongoSchema(TestMongo):
    # TODO test union and optionals
    # TODO array/list testing

    def test_simple_repeat_success(self):
        MongoCollection[Doc1](self.db, self.test_name).insert_one(Doc1(f1='str', f2=1).dict)

    def test_simple_success(self):
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

#     # TODO test indexes
# class TestMongoIndex(TestMongo):

#     def test_simple_none_success(self):
#         coll = Doc1.create_collection(self.db)
#         self.assertEqual(first=(idxinfo:=coll.index_information()), msg=idxinfo, second={
#             '_id_': {
#                 'v': 2,
#                 'key': [('_id', 1)]}})

#     def test_simple_single_default_success(self):
#         class Doc1SingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc.create('new_f1'))):
#             new_f1 :int # NOTE Needed because docs cannot have the same fields
#         coll = Doc1SingleDefaultIndex.create_collection(self.db)
#         self.assertEqual(first=(idxinfo:=coll.index_information()), msg=idxinfo, second={
#             '_id_': {
#                 'key': [('_id', 1)],
#                 'v': 2},
#             'new_f1_1': {
#                 'key': [('new_f1', 1)],
#                 'v': 2}})

#     def test_simple_single_unique_success(self):
#         class Doc1SingleUniqueIndex(Doc1, colldoc=CollDoc(index=IndexDoc.create('new_f4', unique=True))):
#             new_f4 :int # NOTE Needed because docs cannot have the same fields
#         coll = Doc1SingleUniqueIndex.create_collection(self.db)
#         self.assertEqual(first=(idxinfo:=coll.index_information()), msg=idxinfo, second={
#             '_id_': {
#                 'key': [('_id', 1)],
#                 'v': 2},
#             'new_f4_1': {
#                 'key': [('new_f4', 1)],
#                 'v': 2,
#                 'unique': True}})

#     def test_new_options_failure(self):
#         class Doc1SingleDefaultIndexRepeat(Doc1, colldoc=CollDoc(index=IndexDoc.create('new_f1'))):
#             new_f2 :int # NOTE Needed because docs cannot have the same fields
#         Doc1SingleDefaultIndexRepeat.create_collection(self.db)
#         Doc1SingleDefaultIndexRepeat.colldoc = CollDoc(index=IndexDoc.create('new_f1', unique=True))
#         with self.assertRaises(OperationFailure):
#             Doc1SingleDefaultIndexRepeat.create_collection(self.db)


# ### Graveyard
# #     def test_simple_unique_success(self):
# #         pass
#         # coll = self.dbc['db'][f'mycoll{time.perf_counter()//1}']
#         # print()
#         # print(coll.index_information(), end='\n')
#         # coll.create_index('f1')
#         # print(coll.index_information(), end='\n')
#         # class Doc1SingleDefaultUniqueIndex(Doc1, colldoc=CollDoc(index=IndexDoc(args=['f1'], kwargs=to_kwarg(unique=True)))):
#         #     pass
#         # coll = Doc1SingleDefaultUniqueIndex.create_collection(self.db)
#         # self.assertEqual(first=(idxinfo:=coll.index_information()), msg=idxinfo, second={
#         #     '_id_': {
#         #         'v': 2,
#         #         'key': [('_id', 1)]},
#         #     'f1_1_f2_1': {
#         #         'v': 2,
#         #         'key': [('f1', 1), ('f2', 1)],
#         #         'unique': True},
#         #     'f1_1': {
#         #         'v': 2,
#         #         'key': [('f1', 1)]}})


#     # def test_simple_empty_failure(self):
#     #     class Doc1EmptyIndex(Doc1, colldoc=CollDoc(index=IndexDoc.create())):
#     #         pass
#     #     with self.assertRaises(TypeError):
#     #         Doc1EmptyIndex.create_collection(self.db)

#     # def test_simple_default_single_success(self):
#     #     class Doc1SingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc(args=['f1'], kwargs={}))):
#     #         pass
#     #     Doc1SingleDefaultIndex.create_collection(self.db)
#     #     Doc1SingleDefaultIndex.create_collection(self.db)
#     #     Doc1SingleDefaultIndex.create_collection(self.db)

#     # def test_simple_default_single_repeat_failure(self):
#     #     class Doc1aSingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc(args=['f1'], kwargs={}))):
#     #         pass
#     #     Doc1aSingleDefaultIndex.create_collection(self.db)
#     #     # with self.assertRaises(OperationFailure):
#     #     #     Doc1aSingleDefaultIndex.create_collection(self.db)

#     # def test_simple_default_compound(self):
#     #     class Doc1SingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc(args=[['f1', 'f2']], kwargs=to_kwarg(unique=True)))):
#     #         pass
#     #     Doc1SingleDefaultIndex.create_collection(self.db)
#     #     Doc1SingleDefaultIndex.create_collection(self.db)