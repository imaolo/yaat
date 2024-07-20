from yaat.maester import Maester, InformerDoc, PredictionDoc
from yaat.informer import InformerArgs, Informer
from dataclasses import asdict
from bson import Int64
from pathlib import Path
from datetime import datetime
import unittest, os, time, tempfile, torch, copy, pymongo.errors as mongoerr, pandas as pd, numpy as np

class TestMaester(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # setup the maester 
        cls.maester = Maester(connstr=os.getenv('CONNSTR'), dbdir=os.getenv('DBDIR'))

        # setup some data
        cls.df = pd.DataFrame(np.random.randn(500, 3), columns=['A', 'B', 'date'])
        cls.df_fp = Path(tempfile.NamedTemporaryFile(suffix='.csv').name)
        cls.df.to_csv(cls.df_fp)
        cls.required_args = {'root_path': str(cls.df_fp.parent), 'data_path': str(cls.df_fp.name), 'target': 'A'}

        # setup a document
        cls.informer_doc = InformerDoc(**cls.required_args, **Informer.small_scale_args, tickers=['str'], name=time.time(),
                                       num_params=Int64(1), fields=['field'])

        # mongo uses millisecond precision
        cls.informer_doc.date = cls.informer_doc.date.replace(microsecond=int(cls.informer_doc.date.microsecond / 1000) * 1000)

    def setUp(self):
        self.informer_doc.name = f"{time.time()}_name"

    def test_informer_doc_error(self):
        with self.assertRaises(TypeError):
            InformerDoc(root_path='somepath', data_path='somepath', target='targ')

    def test_informer_doc_insert(self):
        self.maester.informers.insert_one(asdict(self.informer_doc))
        retdoc = InformerDoc(**self.maester.informers.find_one({'name': self.informer_doc.name}, {'_id': 0}))
        self.assertEqual(retdoc, self.informer_doc)

    def test_informer_doc_insert_error(self):
        informer_doc = copy.copy(self.informer_doc)
        informer_doc.tickers = [1]
        with self.assertRaises(mongoerr.WriteError):
            self.maester.informers.insert_one(asdict(informer_doc))

    def test_training_update(self):
        # insert and make sure there is not train loss
        self.maester.informers.insert_one(asdict(self.informer_doc))
        retdoc = self.maester.informers.find_one({'name': self.informer_doc.name})
        self.assertIsNone(retdoc['train_loss'])

        # train and update
        informer = Informer(self.informer_doc)
        for update in informer.train():
            update_res = self.maester.informers.update_one({'name': self.informer_doc.name}, {'$set': update})
            self.assertEqual(update_res.modified_count, 1)

        # make sure update occurred
        retdoc = self.maester.informers.find_one({'name': self.informer_doc.name})
        self.assertIsNotNone(retdoc['train_loss'])

    def test_set_informer_weights(self):
        # create informer
        informer = Informer(self.informer_doc)

        # insert informer
        self.maester.informers.insert_one(asdict(self.informer_doc))

        # get the doc
        retdoc = self.maester.informers.find_one({'name': self.informer_doc.name})

        # should have no weights
        self.assertIsNone(retdoc['weights_file_id'])

        # update the weights
        fid = self.maester.set_informer_weights(self.informer_doc.name, informer)

        # create new model
        informer2 = Informer(self.informer_doc)

        # load weights.
        informer2.load_weights(self.maester.fs.get(fid).read())

        # compare weights
        torch.testing.assert_close(informer.exp_model.model.projection.weight, informer2.exp_model.model.projection.weight)

    def test_get_dataset(self):
        df = self.maester.get_dataset(['SPY'])

        ticks = set(col.split('_')[0] for col in df.columns if col != 'date') - {'date'}
        self.assertEqual(len(ticks), 1, msg=ticks)
        self.assertEqual(ticks.pop(), 'SPY')

        df = self.maester.get_dataset(['SPY'], ['open'])
        fields = set(col.split('_')[1] for col in df.columns if col != 'date')
        self.assertEqual(len(fields), 1, msg=fields)
        self.assertEqual(fields.pop(), 'open')

    def test_prediction_doc(self):
        pred_doc = PredictionDoc(str(time.time()), 'model_name', datetime.now(), datetime.now(), [1.0])
        self.maester.predictions.insert_one(asdict(pred_doc))