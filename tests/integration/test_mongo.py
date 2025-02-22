from tests.common import Doc1, Doc2, Doc1Dict, create_integration_test_class
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
    # TODO array/list testing

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
# class TestMongoIndex(TestMongo):

#     def setUp(self):
#         super().setUp()
#         self.db = self.dbc[type(self).__name__]

#     def test_simple_no_index_success(self):
#         class Doc1SingleNoIndex(Doc1):
#             pass
#         coll = Doc1SingleNoIndex.create_collection(self.db)
#         self.assertEqual(first=(idxinfo:=coll.index_information()), msg=idxinfo, second={
#             '_id_': {'v': 2,'key': [('_id', 1)]}})

#     def test_simple_unique_success(self):
#         pass
        # coll = self.dbc['db'][f'mycoll{time.perf_counter()//1}']
        # print()
        # print(coll.index_information(), end='\n')
        # coll.create_index('f1')
        # print(coll.index_information(), end='\n')
        # class Doc1SingleDefaultUniqueIndex(Doc1, colldoc=CollDoc(index=IndexDoc(args=['f1'], kwargs=to_kwarg(unique=True)))):
        #     pass
        # coll = Doc1SingleDefaultUniqueIndex.create_collection(self.db)
        # self.assertEqual(first=(idxinfo:=coll.index_information()), msg=idxinfo, second={
        #     '_id_': {
        #         'v': 2,
        #         'key': [('_id', 1)]},
        #     'f1_1_f2_1': {
        #         'v': 2,
        #         'key': [('f1', 1), ('f2', 1)],
        #         'unique': True},
        #     'f1_1': {
        #         'v': 2,
        #         'key': [('f1', 1)]}})


    # def test_simple_empty_failure(self):
    #     class Doc1EmptyIndex(Doc1, colldoc=CollDoc(index=IndexDoc.create())):
    #         pass
    #     with self.assertRaises(TypeError):
    #         Doc1EmptyIndex.create_collection(self.db)

    # def test_simple_default_single_success(self):
    #     class Doc1SingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc(args=['f1'], kwargs={}))):
    #         pass
    #     Doc1SingleDefaultIndex.create_collection(self.db)
    #     Doc1SingleDefaultIndex.create_collection(self.db)
    #     Doc1SingleDefaultIndex.create_collection(self.db)

    # def test_simple_default_single_repeat_failure(self):
    #     class Doc1aSingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc(args=['f1'], kwargs={}))):
    #         pass
    #     Doc1aSingleDefaultIndex.create_collection(self.db)
    #     # with self.assertRaises(OperationFailure):
    #     #     Doc1aSingleDefaultIndex.create_collection(self.db)

    # def test_simple_default_compound(self):
    #     class Doc1SingleDefaultIndex(Doc1, colldoc=CollDoc(index=IndexDoc(args=[['f1', 'f2']], kwargs=to_kwarg(unique=True)))):
    #         pass
    #     Doc1SingleDefaultIndex.create_collection(self.db)
    #     Doc1SingleDefaultIndex.create_collection(self.db)