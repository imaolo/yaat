from typing import Optional
from exp.exp_informer import Exp_Informer
import torch

class Informer:

    class Args: pass

    def __init__(self, model:str='informer', data:str='custom', root_path:str='..', data_path:str='spy.csv', features:str='MS',
                 target:str='open', freq:str='h', checkpoints:str='./checkpoints/', seq_len:int=96, label_len:int=48, pred_len:int=24,
                 enc_in:int=6, dec_in:int=6, c_out:int=6, d_model:int=512, n_heads:int=8, e_layers:int=2, d_layers:int=1, s_layers:str='3,2,1',
                 d_ff:int=2048, factor:int=5, padding:int=0, distil:bool=True, dropout:float=0.05, attn:str='prob', embed:str='timeF',
                 activation:str='gelu', output_attention:bool=False, mix:bool=True, cols:Optional[str]=None, num_workers:int=0,
                 train_epochs:int=5, batch_size:int=32, f:int=3, learning_rate:float=0.0001, des:str='test', loss:str='mse', patience:int=3,
                 lradj:str='type1', use_amp:bool=False, inverse:bool=False, use_gpu:bool=False, gpu:int=0, use_multi_gpu:bool=False, devices:str='0,1,2,3'):
        # enc_in, dec_in, and c_out must match input feature width, no reason to parameterize here

        # create the arguments object
        self.args_dict = locals()
        self.args_dict.pop('self', None)
        self.args = self.Args()
        for arg_name, arg_value in self.args_dict.items(): setattr(self.args, arg_name, arg_value)

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

        # create the model
        self.model = Exp_Informer(self.args)