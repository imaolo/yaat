import gdown, os, pandas as pd
from pathlib import Path
from yaat.maester import InformerArgs
from yaat.informer import Informer

SEQ_LEN = int(os.getenv('SEQ_LEN', 12))
LAB_LEN = int(os.getenv('LAB_LEN', 6))
PRED_LEN = int(os.getenv('PRED_LEN', 12))
D_MODEL = int(os.getenv('D_MODEL', 128))
N_HEADS = int(os.getenv('N_HEADS', 12))
D_FF = int(os.getenv('D_FF', 12))
E_LAYERS = int(os.getenv('E_LAYERS', 2))
D_LAYERS = int(os.getenv('D_LAYERS', 2))
DROPOUT = float(os.getenv('DROPOUT', 0.1))


# get the data
if not (p:=Path('../spy.csv')).exists():
    gdown.download('https://drive.google.com/uc?id=16V4ZDbx_dfGL4Ky1j1FWxYaWyxG6GGra', str(p), quiet=False)
df = pd.read_csv(str(p))
df.rename(columns={'window_start':'date'}, inplace=True)
df.to_csv(str(p))

# create the doc
informer_args = InformerArgs(
    root_path=str(p.parent),
    data_path=str(p.name),
    target='open',
    seq_len=SEQ_LEN,
    label_len=LAB_LEN,
    pred_len=PRED_LEN,
    d_model=D_MODEL,
    n_heads=N_HEADS,
    d_ff=D_FF,
    e_layers=E_LAYERS,
    d_layers=D_LAYERS,
    dropout=DROPOUT,
    sample_scale=True,
    freq='t',
    loss='mae',
    use_gpu=True)

# create the model
informer = Informer(informer_args)

list(informer.train())
