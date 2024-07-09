from typing import Optional
from dataclasses import dataclass
from exp.exp_informer import Exp_Informer
from pathlib import Path
import torch, io, copy, time

@dataclass
class InformerArgs:
    model: str = 'informer'; data: str = 'custom'; root_path: str = '..'; data_path: str = 'spy.csv'; features: str = 'MS'; target: str = 'open'
    freq: str = 'h'; checkpoints: str = './checkpoints/'; seq_len: int = 96; label_len: int = 48; pred_len: int = 24; enc_in: int = 6; dec_in: int = 6
    c_out: int = 6; d_model: int = 512; n_heads: int = 8; e_layers: int = 2; d_layers: int = 1; s_layers: str = '3,2,1'; d_ff: int = 2048; factor: int = 5
    padding: int = 0; distil: bool = True; dropout: float = 0.05; attn: str = 'prob'; embed: str = 'timeF'; activation: str = 'gelu'; output_attention: bool = False
    mix: bool = True; cols: Optional[str] = None; num_workers: int = 0; train_epochs: int = 5; batch_size: int = 32; patience: int = 3; learning_rate: float = 0.0001
    des: str = 'test'; loss: str = 'mse'; lradj: str = 'type1'; use_amp: bool = False; inverse: bool = False; use_gpu: bool = False; gpu: int = 0
    use_multi_gpu: bool = False; devices: str = '0,1,2,3'

class Informer:

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

        self.settings = '{}_{}_ft{}_sl{}_ll{}_pl{}_dm{}_nh{}_el{}_dl{}_df{}_at{}_fc{}_eb{}_dt{}_mx{}_{}'.format(
            self.args.model, self.args.data, self.args.features, self.args.seq_len, self.args.label_len, self.args.pred_len,
            self.args.d_model, self.args.n_heads, self.args.e_layers, self.args.d_layers, self.args.d_ff, self.args.attn,
            self.args.factor, self.args.embed, self.args.distil, self.args.mix, self.args.des)

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

    def train(self) -> Path: self.exp_model.train(self.settings)

    def test(self) -> Path: self.exp_model.test(self.settings)

    def predict(self) -> Path: self.exp_model.predict(self.settings)