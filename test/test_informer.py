from yaat.informer import Informer, InformerArgs
from pathlib import Path
import unittest, tempfile, numpy as np, pandas as pd, torch

class TestInformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.df = pd.DataFrame(np.random.randn(400, 3), columns=['A', 'B', 'date'])
        cls.df_fp = Path(tempfile.NamedTemporaryFile(suffix='.csv').name)
        cls.df.to_csv(cls.df_fp)
        cls.required_args = {'root_path': cls.df_fp.parent, 'data_path': cls.df_fp.name, 'target': 'A'}

    def test_informer(self):
        informer_args = InformerArgs(**self.required_args, **Informer.small_scale_args)
        informer = Informer(informer_args)

        list(informer.exp_model.train(informer.settings))
        informer.exp_model.predict(informer.settings)

    def test_informer_sample_scale(self):
        informer_args = InformerArgs(**self.required_args, **Informer.small_scale_args, sample_scale=True)
        informer = Informer(informer_args)

        list(informer.train())
        informer.predict()

    def test_load_and_save(self):
        informer = Informer(InformerArgs(**self.required_args, **Informer.small_scale_args, sample_scale=True))
        pre = informer.exp_model.model.projection.weight
        informer.load_weights(informer.byte_weights)
        torch.testing.assert_close(pre, informer.exp_model.model.projection.weight)