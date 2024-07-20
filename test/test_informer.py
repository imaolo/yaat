from yaat.informer import Informer, InformerArgs
from pathlib import Path
import unittest, tempfile, numpy as np, pandas as pd

class TestInformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.df = pd.DataFrame(np.random.randn(400, 3), columns=['A', 'B', 'date'])
        cls.df_fp = Path(tempfile.NamedTemporaryFile(suffix='.csv').name)
        cls.df.to_csv(cls.df_fp)
        cls.informer_args = InformerArgs(cls.df_fp.parent, cls.df_fp.name, 'A', seq_len=4, pred_len=4, label_len=2, d_model=2, train_epochs=1, n_heads=1, d_ff=2)
        cls.informer = Informer(cls.informer_args)

    def test_informer(self):
        list(self.informer.exp_model.train(self.informer.settings))
        self.informer.exp_model.predict(self.informer.settings)