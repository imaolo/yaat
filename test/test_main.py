from yaat.main import parse_args, train
import unittest

class TestMain(unittest.TestCase):

    def test_parse_args(self):
        args = parse_args('train', {'name': 'model_name', 'tickers': 'SPY'})

        # test what was passed
        self.assertEqual(args.cmd, 'train')
        self.assertEqual(args.name, 'model_name')
        self.assertEqual(args.tickers, ['SPY'])

        # test a default
        self.assertEqual(args.d_model, 512)
    
    def test_train(self):
        args = parse_args('train', {'name': 'model_name', 'tickers': 'SPY'})
        train(args)
        