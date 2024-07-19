from yaat.informer import Informer, InformerArgs
import unittest

class TestInformer(unittest.TestCase):

    def test_informer_args(self):
        # test required arguments
        with self.assertRaises(TypeError): InformerArgs()

        # test succesfull with positional
        InformerArgs('root_path_arg', 'data_path_arg')

        # test succesfull with named
        InformerArgs(root_path='root_path_arg', data_path='data_path_arg')