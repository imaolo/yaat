from yaat.main import parse_args, train, maester
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
        args = parse_args('train', {'name': 'model_name', 'tickers': 'SPY'})

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

        
        