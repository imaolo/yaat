import unittest, os, sys, random, pickle
from yaat.util import rm, path, exists
from yaat.maester import Attribute

class TestAttribute(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = 'twork_'+cls.__name__
        if os.path.isdir(cls.dp): rm(cls.dp)
        os.mkdir(cls.dp)

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.isdir(cls.dp): rm(cls.dp)

    def setUp(self) -> None: self.data = ''.join([str(i) for i in range(random.randint(2, 15))])

    def create_attr(self, name:str, *args, **kwargs): return Attribute(path(self.dp, name), *args, **kwargs)

    def test_attr_file_empty(self):
        if exists(p:=path(self.dp, 'test_attr_def_empty_file')): rm(p)
        attr = self.create_attr('test_attr_def_empty')
        self.assertTrue(attr.fp.split('/')[-1] in os.listdir(self.dp))
        with open(attr.fp, 'r') as f: self.assertEqual(f.read(), str(None))

    def test_attr_file_simple_write(self):
        attr = self.create_attr('test_attr_def_simple_data_file', data=self.data)
        with open(attr.fp, 'r') as f: self.assertEqual(f.read(), self.data)

    def test_attr_file_write_twice(self):
        attr = self.create_attr('test_attr_def_file_write', data=self.data)
        attr.data = self.data+'1'
        with open(attr.fp, 'r') as f: self.assertEqual(f.read(), self.data+'1')

    def test_attr_mem_th_not_enough(self):
        attr = self.create_attr('test_attr_mem_th_not_enough', data=self.data, mem_th=sys.getsizeof(self.data))
        with open(attr.fp, 'r') as f: self.assertEqual(f.read(), self.data)
        self.assertIsNone(attr._data)

    def test_attr_mem_th_not_enough(self):
        attr = self.create_attr('test_attr_mem_th_not_enough', data=self.data, mem_th=sys.getsizeof(self.data)+1)
        with open(attr.fp, 'r') as f: self.assertEqual(f.read(), self.data)
        self.assertIsNotNone(attr._data)

    def test_attr_readonly(self):
        attr = self.create_attr('test_attr_readonly', data=self.data, readonly=True)
        with open(attr.fp, 'r') as f: self.assertEqual(f.read(), self.data)
        with self.assertRaises(AssertionError): attr.data = 1

    def test_attr_append(self):
        attr = self.create_attr('test_attr_append', data=self.data, append=True)
        attr.data = self.data
        with open(attr.fp, 'r') as f: self.assertEqual(f.read(), self.data+self.data)

    def test_attr_dir_simple(self):
        attr = self.create_attr('test_attr_dir', data=self.data, is_dir=True)
        self.assertTrue(os.path.isdir(attr.fp))

    def test_attr_deleter(self):
        attr = self.create_attr('test_attr_deleter', data=self.data)
        self.assertTrue(os.path.isfile(attr.fp))
        del attr.data
        self.assertFalse(os.path.isfile(attr.fp))

    def test_attr_pickle(self):
        attr1 = self.create_attr('test_attr_deleter', data=self.data)
        with open(path(self.dp, 'test_attr_deleter.pk'), 'wb') as f: pickle.dump(attr1, f)
        with open(path(self.dp, 'test_attr_deleter.pk'), 'rb') as f: attr2 = pickle.load(f)
        self.assertEqual(attr1.data, attr2.data)
# TODO
# # class TestEntry(unittest.TestCase):
# #     test_num:int=0
# #     root = path(Maester.root, Entry.root)

#     @staticmethod
#     def rm(p:str):
#         if os.path.isdir(p): runcmd(f"rm -rf {p}")
#         elif os.path.isfile(p): os.remove(p)

#     def setUp(self, mem_th=Entry.def_mem_threshold):
#         type(self).test_num+=1
#         self.e = Entry("test"+str(type(self).test_num), mem_th=mem_th)
#         self.wattr = Attribute(self.e, 'a1', 1)
#         self.rattr = Attribute(self.e, 'a2', 2, readonly=True)
#     def tearDown(self):
#         self.rm(self.e.fp)
#         del self.e

#     ### Tests ### 

#     def test_wattr_init(self):
#         self.assertEqual(self.wattr.data, '1')

#     def test_wattr_write(self):
#         self.wattr.data = 2
#         self.assertEqual(self.wattr.data, '2')

#     def test_rattr_init(self):
#         self.assertEqual(self.rattr.data, '2')
    
#     def write_rarttr_werr(self):
#         with self.assertRaises(AssertionError): self.rattr = 1

#     def test_dir_regattr1(self):
#         self.rm(path(self.root, dir:="test_dir_regattr1"))
#         dattr = Attribute(self.e, dir, is_dir=True, exists_ok=False)
#         self.assertTrue(dattr.name in os.listdir(path(Maester.root, self.e.fp)))

#     def test_dir_regattr2(self):
#         self.rm(path(self.root, trash_dir:='test_dir_regattr2'))
#         attr = Attribute(self.e, dir, is_dir=True, exists_ok=False)
#         with self.assertRaises(FileExistsError):
#             self.attr
#             self.e.regattr(trash_dir, trash_dir, is_dir=True, exists_ok=False)

#     def test_append_file1(self):
#         self.rm(attr_fp:=path(self.root, attr:='attr'))
#         self.e.regattr(attr, data:='12234', append=True)
#         self.assertEqual(self.e.attr, data)
#         with open(attr_fp) as f: self.assertEqual(f.read(), data)
#         self.e.attr = data
#         with open(attr_fp) as f: self.assertEqual(f.read(), data+data)

#     def test_write_file1(self):
#         self.rm(attr_fp:=path(self.root, attr:='attr'))
#         self.e.regattr(attr, data:='12234')
#         self.e.attr = data
#         with open(attr_fp) as f: self.assertEqual(f.read(), data)

#     def test_getattr_file(self):
#         self.tearDown()
#         self.setUp(mem_th=100)
#         self.rm(attr_fp:=path(self.root, attr:='attr'))
#         self.e.regattr(attr, data:=str([str(i) for i in range(500)]))
#         self.assertEqual(self.e.attr, data)
#         self.assertTrue(attr not in self.__dict__)

#     def test_set_error(self):
#         errm = "foo"
#         self.assertEqual(self.e.status.data, self.e.Status.created.name)
#         self.e.set_error(errm)
#         self.assertEqual(self.e.status, self.e.Status.error.name)
#         with open(path(Maester.root, self.e.root, self.e.name+"_error"), 'r') as f:
#             self.assertEqual(f.read(), errm)

# # class TestModelEntry(unittest.TestCase):
# #     def test_model_entry_simple(self): ModelEntry("model_test", {'arg':1})

# # class TestDataEntry(unittest.TestCase):
# #     def test_data_entry_simple(self): DataEntry("model_test", {'arg':1})