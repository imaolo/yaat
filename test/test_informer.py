from yaat.informer import Informer, InformerArgs
from pathlib import Path
import unittest, tempfile, numpy as np, pandas as pd

class TestInformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.df = pd.DataFrame(np.random.randn(100, 3), columns=['A', 'B', 'date'])
        cls.df_fp = Path(tempfile.NamedTemporaryFile(suffix='.csv').name)
        cls.df.to_csv(cls.df_fp)
        cls.informer = Informer(InformerArgs(cls.df_fp.parent, cls.df_fp.name, 'A'))

    def test_informer(self):
        self.informer.exp_model.train(self.informer.settings)
        self.informer.exp_model.predict(self.informer.settings)