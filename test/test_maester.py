import unittest, os, random, pickle, time
from yaat.util import rm, path, exists, getenv, read, objsz, exists, mkdirs, dict2str
from yaat.maester import Attribute, Entry, ModelEntry, DataEntry
from typing import Any

DEBUG=getenv("DEBUG", 0)

def getid(tc:unittest.TestCase): return tc.id().split('.')[-1]

class TestAttribute(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = f"twork_{cls.__name__}_{int(time.time()*1e3)}"
        if exists(cls.dp): rm(cls.dp)
        mkdirs(cls.dp)
    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: rm(cls.dp)

    def setUp(self) -> None:
        self.data = ''.join([str(i) for i in range(random.randint(2, 5))])
        self.attr = self.create_attr(f"{getid(self)}_{int(time.time()*1e3)}", self.data)
    def tearDown(self) -> None: del self.attr.data
    
    def create_attr(self, name:str, data:Any, *args, **kwargs): return Attribute(path(self.dp, name), data, *args, **kwargs)

    ### Tests ###

    def test_empty(self):
        if exists(p:=path(self.dp, getid(self))): rm(p)
        attr = self.create_attr(getid(self), data='')
        self.assertTrue(attr.fp.split('/')[-1] in os.listdir(self.dp))
        self.assertEqual(read(attr.fp), '')

    def test_write(self):
        self.assertEqual(read(self.attr.fp), self.data)
        self.assertEqual(read(self.attr.fp), self.attr.data, msg=f"{type(self.attr.data)}")

    def test_write_twice(self):
        self.attr.buf = '1432543254325432'
        self.assertEqual(read(self.attr.fp), '1432543254325432')

    def test_mem_th_not_enough(self):
        attr = self.create_attr(getid(self), self.data, mem_th=objsz(self.data))
        self.assertIsNone(attr._buf, msg=f"{objsz(attr._buf)}, {attr._buf}, {attr.mem_th}, {objsz(read(attr.fp))}")

    def test_mem_th_enough(self):
        attr = self.create_attr(getid(self), self.data, mem_th=objsz(self.data)+1)
        self.assertIsNotNone(attr._buf)

    def test_readonly(self):
        attr = self.create_attr(getid(self), self.data, readonly=True)
        self.assertEqual(read(attr.fp), self.data)
        with self.assertRaises(AssertionError): attr.buf = attr.data
        with self.assertRaises(AssertionError): attr.buf += attr.data

    def test_append(self):
        self.attr.buf += self.attr.data
        self.assertEqual(self.attr.data, self.data+'\n'+self.data)

    def test_delete(self):
        attr = self.create_attr(getid(self), self.data)
        self.assertTrue(os.path.isfile(attr.fp))
        del attr.data
        self.assertFalse(os.path.isfile(attr.fp))

    def test_pickle(self):
        attr1 = self.create_attr(getid(self), self.data)
        with open(path(self.dp, f'{getid(self)}.pk'), 'wb') as f: pickle.dump(attr1, f)
        with open(path(self.dp, f'{getid(self)}.pk'), 'rb') as f: attr2 = pickle.load(f)
        self.assertEqual(attr1.data, attr2.data)

    def test_appendonly(self):
        attr = self.create_attr(getid(self), self.data, appendonly=True)
        with self.assertRaises(AssertionError): attr.buf = 1
        attr.buf += self.data
        self.assertEqual(attr.data, self.data+'\n'+self.data)

class TestEntry(unittest.TestCase):
    test_num:int = 0

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = f"twork_{cls.__name__}_{int(time.time()*1e3)}"
        if exists(cls.dp): rm(cls.dp)
        mkdirs(cls.dp)
    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: rm(cls.dp)

    def setUp(self) -> None:
        self.entry = Entry(path(self.dp, f"{getid(self)}_{int(time.time()*1e3)}"))
    def tearDown(self) -> None:
        rm(self.entry.fp)
        del self.entry

    ### Tests ###

    def test_constructor(self):
        self.assertTrue(os.path.isdir(self.entry.fp))
        self.assertEqual(self.entry.status.data, Entry.Status.created.name)
        with open(self.entry.status.fp, 'r') as f : self.assertEqual(f.read(), Entry.Status.created.name)

    def test_status_update(self):
        self.entry.status.buf += Entry.Status.running.name
        with open(self.entry.status.fp, 'r') as f : self.assertEqual(f.read().split('\n')[-1], Entry.Status.running.name)

    def test_set_error(self):
        self.entry.set_error(msg1:=f"pytorch error: {'blah blah pytorch failed'}")
        self.assertEqual(read(path(self.entry.fp, 'error_0')), msg1)
        self.assertEqual(read(self.entry.status.fp).split('\n')[-1], Entry.Status.error.name)
        self.entry.set_error(msg2:=f"pytorch error: {'blah blah pytorch failed number 2'}")
        self.assertEqual(read(path(self.entry.fp, 'error_1')), msg2)
        self.assertEqual(read(path(self.entry.fp, 'error_0')), msg1)
        self.assertEqual(read(self.entry.status.fp).split('\n')[-1], Entry.Status.error.name)

class TestModelEntry(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = f"twork_{cls.__name__}_{int(time.time()*1e3)}"
        if exists(cls.dp): rm(cls.dp)
        mkdirs(cls.dp)
    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: rm(cls.dp)

    def setUp(self) -> None:
        self.args = {'a':'d'}
        self.me = ModelEntry(path(self.dp, f"{getid(self)}_{int(time.time()*1e3)}"), self.args)
    def tearDown(self) -> None: rm(self.me.fp)

    def test_simple(self):
        self.assertEqual(self.me.args.data, dict2str(self.args))

class TestDataEntry(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = f"twork_{cls.__name__}_{int(time.time()*1e3)}"
        if exists(cls.dp): rm(cls.dp)
        mkdirs(cls.dp)
    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: rm(cls.dp)

    def setUp(self) -> None:
        self.data = 'fdafdasfdsa'
        self.de = DataEntry(path(self.dp, f"{getid(self)}_{int(time.time()*1e3)}"), self.data)
    def tearDown(self) -> None: rm(self.de.fp)

    def test_simple(self):
        self.assertEqual(self.de.data.data, self.data)