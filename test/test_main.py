from yaat.main import parse_args, train, predict, maester
from pathlib import Path
from yaat.informer import Informer
import unittest, time, tempfile, pandas as pd, numpy as np

class TestMain(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.df = pd.DataFrame(np.random.randn(400, 3), columns=['A', 'B', 'date'])
        cls.df_fp = Path(tempfile.NamedTemporaryFile(suffix='.csv').name)
        cls.df.to_csv(cls.df_fp)
        cls.required_args = {'root_path': cls.df_fp.parent, 'data_path': cls.df_fp.name, 'target': 'A'}

    def test_parse_args(self):
        args = parse_args('train', {'name': 'model_name', 'tickers': 'SPY', 'target': 'SPY_open'})

        # test what was passed
        self.assertEqual(args.cmd, 'train')
        self.assertEqual(args.name, 'model_name')
        self.assertEqual(args.tickers, ['SPY'])

        # test a default
        self.assertEqual(args.d_model, 512)
    
    def test_train(self):
        args = {'name': str(time.time()), 'tickers': 'SPY', 'target': 'SPY_open'} \
                | dict(map(lambda kv: (kv[0], str(kv[1])), Informer.small_scale_args.items()))
        args = parse_args('train', args)
        train(args)

        doc = maester.informers.find_one({'name': args.name})
        self.assertIsNotNone(doc['train_loss'])

    def test_train_sample_scale(self):
        args = {'name': str(time.time()), 'tickers': 'SPY', 'target': 'SPY_open'} \
                | dict(map(lambda kv: (kv[0], str(kv[1])), Informer.small_scale_args.items()))
        args = parse_args('train', args)
        args.sample_scale=True
        args.pred_len = 12
        args.seq_len = 12
        train(args)

        doc = maester.informers.find_one({'name': args.name})
        self.assertIsNotNone(doc['train_loss'])


    def test_predict(self):
        train_args = {'name': str(time.time()), 'tickers': 'SPY', 'target': 'SPY_open'} \
                | dict(map(lambda kv: (kv[0], str(kv[1])), Informer.small_scale_args.items()))
        train_args = parse_args('train', train_args)
        train(train_args)

        pred_args = {'model_name': train_args.name, 'name': str(time.time()), 'start_date': '2023-7-13'}
        pred_args = parse_args('predict', pred_args)
        predict(pred_args)

    def test_predict_sample_scale(self):
        train_args = {'name': str(time.time()), 'tickers': 'SPY', 'target': 'SPY_open'} \
                | dict(map(lambda kv: (kv[0], str(kv[1])), Informer.small_scale_args.items()))
        train_args = parse_args('train', train_args)
        train_args.sample_scale=True
        train_args.pred_len = 12
        train_args.seq_len = 12
        train(train_args)

        pred_args = {'model_name': train_args.name, 'name': str(time.time()), 'start_date': '2023-7-13'}
        pred_args = parse_args('predict', pred_args)
        predict(pred_args)
        

        