from typing import Optional, List, Dict, Any
from dataclasses import dataclass, fields
from exp.exp_informer import Exp_Informer
from pathlib import Path
import torch, io, copy, time, numpy as np, pandas as pd

@dataclass(kw_only=True)
class InformerArgs:
    # required
    root_path: str
    data_path: str
    target: str

    # defaults
    model: str = 'informer'
    freq: str = 'h'
    checkpoints: str = './checkpoints/'
    seq_len: int = 96
    label_len: int = 48
    pred_len: int = 24
    d_model: int = 512
    n_heads: int = 8
    e_layers: int = 2
    d_layers: int = 1
    s_layers: str = '3,2,1'
    d_ff: int = 2048
    factor: int = 5
    padding: int = 0
    distil: bool = True
    dropout: float = 0.05
    attn: str = 'prob'
    embed: str = 'timeF'
    activation: str = 'gelu'
    output_attention: bool = False
    mix: bool = True
    cols: Optional[str] = None
    num_workers: int = 0
    train_epochs: int = 5
    batch_size: int = 32
    patience: int = 3
    learning_rate: float = 0.0001
    des: str = 'test'
    loss: str = 'mse'
    lradj: str = 'type1'
    use_amp: bool = False
    inverse: bool = False
    use_gpu: bool = False
    gpu: int = 0
    use_multi_gpu: bool = False
    devices: str = '0,1,2,3'
    sample_scale:bool = False

    @classmethod
    def from_dict(cls, args: Dict[str, Any]) -> 'InformerArgs':
        return cls(**({k: v for k, v in args.items() if k in [field.name for field in fields(cls)]}))

class Informer:

    small_scale_args = {'seq_len':4, 'pred_len':4, 'label_len':2, 'd_model':2, 'train_epochs':1, 'n_heads':1, 'd_ff':2}

    def __init__(self, args:InformerArgs):
        # enc_in, dec_in, and c_out must match input feature width, no reason to parameterize here

        # cache arguments
        self.og_args = copy.copy(args)
        self.args = copy.copy(args)

        # record creation time
        self.timestamp = time.time()

        # set these manually
        self.args.use_gpu = torch.cuda.is_available() and self.args.use_gpu
        if self.args.use_gpu and self.args.use_multi_gpu:
            self.args.devices = self.args.devices.replace(' ','')
            device_ids = self.args.devices.split(',')
            self.args.device_ids = [int(id_) for id_ in device_ids]
            self.args.gpu = self.args.device_ids[0]
        self.args.s_layers = [int(s_l) for s_l in self.args.s_layers.replace(' ','').split(',')]
        self.args.detail_freq = self.args.freq
        self.args.freq = self.args.freq[-1:]

        # these args depend on the input data
        cols = pd.read_csv((Path(args.root_path) / args.data_path).resolve(), nrows=0).columns
        self.args.enc_in = self.args.dec_in = len(cols)-1

        # should never change but are required
        self.args.c_out = 1
        self.args.features = 'MS'
        self.args.data = 'custom'

        self.settings = '{}_{}_ft{}_sl{}_ll{}_pl{}_dm{}_nh{}_el{}_dl{}_df{}_at{}_fc{}_eb{}_dt{}_mx{}_{}_ts{}'.format(
            self.args.model, self.args.data, self.args.features, self.args.seq_len, self.args.label_len, self.args.pred_len,
            self.args.d_model, self.args.n_heads, self.args.e_layers, self.args.d_layers, self.args.d_ff, self.args.attn,
            self.args.factor, self.args.embed, self.args.distil, self.args.mix, self.args.des, self.timestamp)

        # create the model
        self.exp_model = Exp_Informer(self.args)

    @property
    def weights_file_path(self) -> Path: return Path.cwd() / self.args.checkpoints / self.settings / 'checkpoints.pth'

    @property
    def results_directory_path(self) -> Path: return Path.cwd() / 'results' / self.settings

    @property
    def predictions_file_path(self) -> Path: return Path.cwd() / 'results' / self.settings / 'real_prediction.npy'

    @property
    def byte_weights(self) -> bytes:
        with io.BytesIO() as bytes_io:
            torch.save(self.exp_model.model.state_dict(), bytes_io)
            return bytes_io.getvalue()

    @property
    def num_params(self) -> int: return sum([p.numel() for p in self.exp_model.model.parameters() if p.requires_grad])