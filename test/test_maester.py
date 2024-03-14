import unittest, os, random
from yaat.util import rm, path, exists, getenv, read, objsz, exists, \
    mkdirs, dict2str, gettime, serialize, write, construct, filesz
from yaat.maester import Attribute, Entry, ModelEntry, DatasetEntry, Maester
from typing import Any
import torch

DEBUG = getenv("DEBUG", 0)

def getid(tc:unittest.TestCase): return tc.id().split('.')[-1]

class TestMaesterSetup(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = f"twork/twork_{cls.__name__}_{gettime()}"
        if exists(cls.dp): rm(cls.dp)
        mkdirs(cls.dp)
    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: rm(cls.dp)

class TestAttribute(TestMaesterSetup):

    def setUp(self) -> None:
        self.data = ''.join([str(i) for i in range(random.randint(2, 5))])
        self.attr = self.create_attr(f"{getid(self)}_{gettime()}", self.data)
    def tearDown(self) -> None: del self.attr.data
    
    def create_attr(self, name:str, data:Any, *args, **kwargs): return Attribute(path(self.dp, name), data, *args, **kwargs)

    ### Tests ###

    def test_empty(self):
        if exists(p:=path(self.dp, getid(self))): rm(p)
        attr = self.create_attr(getid(self), '')
        self.assertTrue(attr.fp.split('/')[-1] in os.listdir(self.dp))
        self.assertEqual(read(attr.fp), '')

    def test_write(self):
        self.assertEqual(read(self.attr.fp), self.data)
        self.assertEqual(read(self.attr.fp), self.attr.data, msg=f"{type(self.attr.data)}")

    def test_write_twice(self):
        self.attr.buf = '1432543254325432'
        self.assertEqual(read(self.attr.fp), '1432543254325432')
        self.assertEqual(self.attr.data, '1432543254325432')

    def test_mem_not_enough(self):
        attr = self.create_attr(getid(self), self.data, mem=objsz(self.data))
        self.assertIsNone(attr._buf, msg=f"{objsz(attr._buf)}, {attr._buf}, {attr.mem}, {objsz(read(attr.fp))}")

    def test_mem_enough(self):
        attr = self.create_attr(getid(self), self.data, mem=objsz(self.data)+1)
        self.assertIsNotNone(attr._buf)

    def test_readonly(self):
        attr = self.create_attr(getid(self), self.data, readonly=True)
        self.assertEqual(read(attr.fp), self.data)
        with self.assertRaises(AssertionError): attr.buf = attr.data
        with self.assertRaises(AssertionError): attr.buf += attr.data

    def test_append(self):
        self.attr.buf += self.attr.data
        self.assertEqual(self.attr.data, self.data+'\n'+self.data)
        self.assertEqual(read(self.attr.fp), self.data+'\n'+self.data)

    def test_delete(self):
        attr = self.create_attr(getid(self), self.data)
        self.assertTrue(os.path.isfile(attr.fp))
        del attr.data
        self.assertFalse(os.path.isfile(attr.fp))

    def test_pickle(self):
        attr1 = self.create_attr(getid(self), self.data, mem=0)
        write(p:=path(self.dp, 'pickle_test'), serialize(attr1), 'wb')
        attr2 = construct(read(p, 'rb'))
        self.assertEqual(attr1.data, attr2.data)

    def test_appendonly(self):
        attr = self.create_attr(getid(self), self.data, appendonly=True)
        with self.assertRaises(AssertionError): attr.buf = 1
        attr.buf += self.data
        self.assertEqual(attr.data, self.data+'\n'+self.data)
        self.assertEqual(read(attr.fp), self.data+'\n'+self.data)

class TestEntry(TestMaesterSetup):
    test_num:int = 0

    def setUp(self) -> None:
        self.entry = Entry(path(self.dp, f"{getid(self)}_{gettime()}"))
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

    def test_pickle(self):
        e1 = Entry(path(self.dp, f"{getid(self)}_{gettime()}"))
        e1.set_error('some messge')
        e1.save()
        e2 = Entry.load(e1.obj.fp)
        self.assertEqual(e1.error.data, e2.error.data)

class Model(torch.nn.Module):
    def __init__(self):
        super(type(self), self).__init__()
        self.linear = torch.nn.Linear(1, 1)
model = Model() 

class TestModelEntry(TestMaesterSetup):

    def setUp(self) -> None:
        self.args = {'a':'d'}
        self.me = ModelEntry(path(self.dp, f"{getid(self)}_{gettime()}"), self.args, model=model)
    def tearDown(self) -> None: rm(self.me.fp)

    ### Tests ###

    def test_args_simple(self):
        self.assertEqual(self.me.args.data, dict2str(self.args))
    
    def test_weights_simple(self):
        self.me = ModelEntry(path(self.dp, f"{getid(self)}_{gettime()}"), self.args, model=model, mem=objsz(model.state_dict()))
        self.assertIsNone(self.me.weights._buf)
        model1 = Model()
        model1.load_state_dict(self.me.weights.data)
        self.assertTrue(torch.equal(model1.linear.weight, model.linear.weight))

class TestDataSetEntry(TestMaesterSetup):

    def setUp(self) -> None:
        self.data = 'fdafdasfdsa'
        self.de = DatasetEntry(path(self.dp, f"{getid(self)}_{gettime()}"), self.data)
    def tearDown(self) -> None: rm(self.de.fp)

    ### Tests ###

    def test_simple(self):
        self.assertEqual(self.de.dataset.data, self.data)

    def test_pickle(self):
        data = bytes(int(1e6))
        de = DatasetEntry(path(self.dp, 'mydataset2'), data=data, mem=0)
        de.save()
        self.assertLess(filesz(de.obj.fp), objsz(data)/3)
        de1 = DatasetEntry.load(de.obj.fp)
        self.assertIsNone(de1.dataset._buf)
        self.assertEqual(de1.dataset.data, data)

class TestMaester(TestMaesterSetup):

    @classmethod
    def setUpClass(cls) -> None:
        cls.dp = f"twork/twork_{cls.__name__}_{gettime()}"
        if exists(cls.dp): rm(cls.dp)
        mkdirs(cls.dp)
        cls.maester = Maester(cls.dp, mem=0)
    @classmethod
    def tearDownClass(cls) -> None:
        if not DEBUG: rm('twork')

    ### Tests ###

    def test_create_model(self):
        args = {'key': 'val'}
        model = Model()
        self.maester.create_model(n:=f"{getid(self)}_{gettime()}", args=args, model=model, mem=0)
        modelentry = self.maester.models[n]
        model1 = Model()
        model1.load_state_dict(modelentry.weights.data)
        self.assertTrue(torch.equal(model1.linear.weight, model.linear.weight))

    def test_create_dataset(self):
        data = '12345'
        self.maester.create_dataset(n:=f"{getid(self)}_{gettime()}", data, mem=0)
        self.assertEqual(data, self.maester.datasets[n].dataset.data)

    def test_sync(self):
        data = '12345'
        self.maester.create_dataset(n:=f"{getid(self)}_{gettime()}", data)
        maester = Maester(self.maester.fp)
        self.assertEqual(maester.datasets[n].dataset.data, data)