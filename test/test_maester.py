from yaat.util import rm, path, exists, getenv, read, objsz, exists, \
    mkdirs, dict2str, gettime, serialize, write, construct, filesz, str2dict, \
    readlines, writelines
from yaat.maester import Attribute, Entry, ModelEntry, DatasetEntry, PredEntry, Maester
from typing import Any
import unittest, torch, random, os, random, numpy as np, pandas as pd


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
        self.assertEqual(self.attr.data, self.data*2)
        self.assertEqual(read(self.attr.fp), self.data*2)

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
        self.assertEqual(attr.data, self.data*2)
        self.assertEqual(read(attr.fp), self.data*2)

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
        self.assertEqual(self.entry.status.data[-1], Entry.Status.created.name)
        self.assertEqual(readlines(self.entry.status.fp)[-1], Entry.Status.created.name)

    def test_status_update(self):
        self.entry.status.buf += [Entry.Status.running.name]
        self.assertEqual(readlines(self.entry.status.fp)[-1], Entry.Status.running.name)

    def test_set_error(self):
        self.entry.set_error(msg1:=f"pytorch error: {'blah blah pytorch failed'}")
        self.assertEqual(read(path(self.entry.fp, 'error_0')), msg1)
        self.assertEqual(readlines(self.entry.status.fp)[-1], Entry.Status.error.name)
        self.entry.set_error(msg2:=f"pytorch error: {'blah blah pytorch failed number 2'}")
        self.assertEqual(read(path(self.entry.fp, 'error_1')), msg2)
        self.assertEqual(read(path(self.entry.fp, 'error_0')), msg1)
        self.assertEqual(readlines(self.entry.status.fp)[-1], Entry.Status.error.name)

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
        self.assertEqual(dict2str(str2dict(self.me.args.data)), dict2str(self.args))
    
    def test_weights_simple(self):
        self.me = ModelEntry(path(self.dp, f"{getid(self)}_{gettime()}"), self.args, model=model, mem=objsz(model.state_dict()))
        self.assertIsNone(self.me.weights._buf)
        model1 = Model()
        model1.load_state_dict(self.me.weights.data)
        self.assertTrue(torch.equal(model1.linear.weight, model.linear.weight))

class TestDataSetEntry(TestMaesterSetup):

    def setUp(self) -> None:
        self.cols = {'c1': pd.Series([], dtype='str'), 'c2': pd.Series([], dtype='int'), 'c3': pd.Series([], dtype='float')}
        self.data = ['c1 val', 1, 1.3]
        self.de = DatasetEntry(path(self.dp, f"{getid(self)}_{gettime()}"), self.cols)
        self.de.dataset.buf += self.data
    def tearDown(self) -> None: rm(self.de.fp)

    ### Tests ###

    def test_simple(self):
        self.assertEqual(self.de.dataset.data.iloc[0].tolist(), self.data)

    def test_pickle(self):
        data = bytes(int(1e3))
        de = DatasetEntry(path(self.dp, 'mydataset2'), {'c': [], 'c2': []}, mem=0)
        de.dataset.buf += pd.DataFrame([[data, data]])
        de.save()
        self.assertIsNone(de.dataset._buf)
        de1 = DatasetEntry.load(de.obj.fp)
        self.assertIsNone(de1.dataset._buf)
        self.assertEqual(de.dataset.data.iloc[0].tolist()[0], str(data))
        self.assertEqual(de.dataset.data.iloc[0].tolist()[1], str(data))

    @unittest.skip("we may not need this")
    def test_mean_std(self):
        cols = ['c1', 'c2', 'c3']
        data = [[random.random() for __ in range(len(cols))] for _ in range(10)]
        de = DatasetEntry(path(self.dp, f"{getid(self)}_{gettime()}"), data=','.join(cols))
        for d in data: de.dataset.buf += ','.join(map(str, d))

        de.preprocess()
        np.testing.assert_allclose(np.array(data).mean(0), de.mean.data, rtol=1e5, atol=1e5)
        np.testing.assert_allclose(np.array(data).std(0), de.std.data, rtol=1e5, atol=1e5)

class TestPredEntry(TestMaesterSetup):
    
    def setUp(self) -> None:
        self.pred = np.array(1)
        self.pe = PredEntry(path(self.dp, f"{getid(self)}_{gettime()}"), self.pred, 'dne_model', 'dne_dataset')
    def tearDown(self) -> None: rm(self.pe.fp)

    ### Tests ###

    def test_simple(self): np.testing.assert_allclose(self.pe.pred.data, self.pred)

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
        cols = {'c1': pd.Series([], dtype='str'), 'c2': pd.Series([], dtype='int'), 'c3': pd.Series([], dtype='float')}
        data = ['c1 val', 1, 1.3]
        self.maester.create_dataset(n:=f"{getid(self)}_{gettime()}", cols, mem=0)
        de = self.maester.datasets[n]
        de.dataset.buf += data
        self.assertEqual(de.dataset.data.iloc[0].tolist(), data)

    def test_create_pred(self):
        pred = np.array(1)
        self.maester.create_pred(n:=f"{getid(self)}_{gettime()}", pred, 'model_name', 'dataset_name')
        np.testing.assert_allclose(self.maester.preds[n].pred.data, pred)
        maester = Maester(self.maester.fp)
        np.testing.assert_allclose(maester.preds[n].pred.data, pred)

    def test_sync(self):
        cols = {'c1': pd.Series([], dtype='str'), 'c2': pd.Series([], dtype='int'), 'c3': pd.Series([], dtype='float')}
        data = ['c1 val', 1, 1.3]
        self.maester.create_dataset(n:=f"{getid(self)}_{gettime()}", cols, mem=0)
        self.maester.datasets[n].dataset.buf += data
        maester = Maester(self.maester.fp)
        self.assertEqual(maester.datasets[n].dataset.data.iloc[0].tolist(), data)