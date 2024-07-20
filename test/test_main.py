from yaat.main import parse_args
import unittest

class TestMain(unittest.TestCase):

    def test_parse_args(self):
        args = parse_args('train', {'name': 'model_name', 'tickers': 'SPY'})

        # test what was passed
        self.assertEqual(args.cmd, 'train')
        self.assertEqual(args.name, 'model_name')
        self.assertEqual(args.tickers, ['SPY'])

        # test some defaults
        self.assertEqual(args.d_model, 512)