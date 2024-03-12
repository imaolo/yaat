import unittest, os, sys, random, pickle, time
from yaat.util import rm, path, exists, getenv, read
from yaat.maester import Attribute
from typing import Any

DEBUG=getenv("DEBUG", 0)

def getid(tc:unittest.TestCase): return tc.id().split('.')[-1]

class TestAttribute(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = f"twork_{cls.__name__}_{int(time.time()*1e3)}"
        if os.path.isdir(cls.dp): rm(cls.dp)
        os.mkdir(cls.dp)
    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: rm(cls.dp)

    def setUp(self) -> None:
        self.data = ''.join([str(i) for i in range(random.randint(2, 5))])
        self.attr = self.create_attr(f"{getid(self)}_{int(time.time()*1e3)}", self.data)
    def tearDown(self) -> None: del self.attr.buf
    
    def create_attr(self, name:str, data:Any, *args, **kwargs): return Attribute(path(self.dp, name), data, *args, **kwargs)

    ### Tests ###

    def test_empty(self):
        if exists(p:=path(self.dp, getid(self))): rm(p)
        attr = self.create_attr(getid(self), data='')
        self.assertTrue(attr.fp.split('/')[-1] in os.listdir(self.dp))
        self.assertEqual(read(attr.fp), '')

    def test_write(self):
        self.assertEqual(read(self.attr.fp), self.data)
        self.assertEqual(read(self.attr.fp), self.attr.buf, msg=f"{type(self.attr.buf)}")

    def test_write_twice(self):
        self.attr.buf = '1432543254325432'
        self.assertEqual(read(self.attr.fp), '1432543254325432')

    def test_mem_th_not_enough(self):
        attr = self.create_attr(getid(self), self.data*10, mem_th=sys.getsizeof(self.data))
        self.assertIsNone(attr._buf, msg=f"{sys.getsizeof(attr._buf)}, {attr._buf}, {attr.mem_th}, {sys.getsizeof(self.data*1000)}")

    def test_mem_th_enough(self):
        attr = self.create_attr(getid(self), self.data, mem_th=sys.getsizeof(self.data)*2)
        self.assertIsNotNone(attr._buf)

    def test_readonly(self):
        attr = self.create_attr(getid(self), self.data, readonly=True)
        self.assertEqual(read(attr.fp), self.data)
        with self.assertRaises(AssertionError): attr.buf = 1

    def test_append(self):
        self.attr.buf += self.data
        self.assertEqual(self.attr.buf, self.data+self.data)

    def test_delete(self):
        attr = self.create_attr(getid(self), self.data)
        self.assertTrue(os.path.isfile(attr.fp))
        del attr.buf
        self.assertFalse(os.path.isfile(attr.fp))

    def test_pickle(self):
        attr1 = self.create_attr(getid(self), self.data)
        with open(path(self.dp, f'{getid(self)}.pk'), 'wb') as f: pickle.dump(attr1, f)
        with open(path(self.dp, f'{getid(self)}.pk'), 'rb') as f: attr2 = pickle.load(f)
        self.assertEqual(attr1.buf, attr2.buf)

# TODO
# class TestEntry(unittest.TestCase):
#     test_num:int = 0

#     @classmethod
#     def setUpClass(cls) -> None:
#         cls.dp = 'twork'+cls.__name__
#         if os.path.isdir(cls.dp): rm(cls.dp)
#         os.mkdir(cls.dp)
#     @classmethod
#     def tearDownClass(cls) -> None:
#         if not DEBUG: rm(cls.dp)

#     def setUp(self) -> None:
#         self.entry = Entry(path(self.dp, f"test_{type(self).__name__}_{self.test_num}"))
#         self.test_num += 1
#     def tearDown(self) -> None:
#         rm(self.entry.dir.fp)
#         del self.entry

#     ### Tests ###

#     def test_constructor(self):
#         self.assertTrue(os.path.isdir(self.entry.dir.fp))
#         self.assertEqual(self.entry.status.data, Entry.Status.created.name)
#         with open(self.entry.status.fp, 'r') as f : self.assertEqual(f.read(), Entry.Status.created.name)

#     def test_status_update(self):
#         self.entry.status.data = Entry.Status.running.name
#         with open(self.entry.status.fp, 'r') as f : self.assertEqual(f.read(), Entry.Status.created.name+Entry.Status.running.name)
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
#         self.assertEqual(self.wattr.buf, '1')

#     def test_wattr_write(self):
#         self.wattr.buf = 2
#         self.assertEqual(self.wattr.buf, '2')

#     def test_rattr_init(self):
#         self.assertEqual(self.rattr.buf, '2')
    
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