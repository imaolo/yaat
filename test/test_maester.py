from yaat.maester import Maester, InformerDoc, PredictionDoc
from yaat.informer import Informer
from dataclasses import asdict
from bson import Int64
from pathlib import Path
from datetime import datetime, timedelta
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
        pred_doc = PredictionDoc(str(time.time()), 'model_name', datetime.now(), [1.0])
        self.maester.predictions.insert_one(asdict(pred_doc))

    def test_get_dataset_date_ranges(self):

        tick='SPY'

        start_date = self.maester.candles_db[tick].find_one(sort=[("date", 1)])['date']
        end_date = self.maester.candles_db[tick].find_one(sort=[("date", -1)])['date']

        df = self.maester.get_dataset([tick], start_date=start_date, end_date=end_date)

        self.assertEqual(df['date'].max(), end_date)
        self.assertEqual(df['date'].min(), start_date)

    def test_create_ticker_dataset(self):
        # cleanup
        ticker = 'SNAP'
        if ticker in self.maester.candles_db.list_collection_names(): self.maester.candles_db[ticker].drop()

        # mine that shit
        self.maester.create_tickers_dataset(ticker, start_date=datetime(2023, 1, 1), end_date=datetime(2023, 1, 5))

        # test
        self.assertEqual(self.maester.candles_db[ticker].count_documents({}), 11661) # gets the whole month

    def test_call_alpha(self):
        self.maester.alpha_call(function='TIME_SERIES_INTRADAY', outputsize='full', extended_hours='true', interval=f'1min', symbol='SPY', month=f"2023-1")

    def test_alpha_get_earliest_date(self):
        tick = 'SPY'
        earliest = self.maester.alpha_get_earliest(tick, start_date=datetime.strptime('2000-3', '%Y-%m'))
        candles = self.maester.alpha_extract_data(self.maester.alpha_call_intraday(tick, earliest))
        time = datetime.strptime(list(candles.keys())[0], '%Y-%m-%d %H:%M:%S')
        self.assertEqual(time.year, earliest.year)
        self.assertEqual(time.month, earliest.month)

        invalid = earliest - timedelta(weeks=2)
        candles = self.maester.alpha_extract_data(self.maester.alpha_call_intraday(tick, invalid))
        time = datetime.strptime(list(candles.keys())[0], '%Y-%m-%d %H:%M:%S')
        self.assertNotEqual(time.year, earliest.year)
        self.assertNotEqual(time.month, earliest.month)

        tick = 'TSLA'
        earliest = self.maester.alpha_get_earliest(tick, start_date=datetime.strptime('2010-07', '%Y-%m'))
        candles = self.maester.alpha_extract_data(self.maester.alpha_call_intraday(tick, earliest))
        time = datetime.strptime(list(candles.keys())[0], '%Y-%m-%d %H:%M:%S')
        self.assertEqual(time.year, earliest.year)
        self.assertEqual(time.month, earliest.month)
        print(earliest)

        # some tickers return current data when out of range, others error, should be dealt with more cleanly
        invalid = earliest - timedelta(weeks=2)
        with self.assertRaises(AssertionError): self.maester.alpha_extract_data(self.maester.alpha_call_intraday(tick, invalid))
