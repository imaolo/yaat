from yaat.util import ROOT, DEBUG, path, filename, dict2str, str2dict
from yaat.maester import Maester, Entry
from Informer2020.exp_informer import Exp_Informer
from typing import Dict, Any, TYPE_CHECKING
import torch

# if TYPE_CHECKING:
from argparse import Namespace

class Informer:
    def __init__(self, maester: Maester, args:Namespace):
        self.args = args
        self.maester = maester

        assert self.args.name not in self.maester.models, f"model {self.args.name} already exists"
        assert self.args.dataset in self.maester.datasets, f"dataset {self.args.dataset} does not exist"
        self.maester.create_model(self.args.name, args=vars(self.args), model=torch.nn.Linear(1, 1))

        me = self.maester.models[self.args.name]
        de = self.maester.datasets[self.args.dataset]

        self.args.checkpoints = me.weights.fp
        self.args.root_path = path(*de.dataset.fp.split('/')[:-1])
        self.args.data_path = filename(de.dataset.fp)
        self.args.data = 'custom'

        self.mdl = Exp_Informer(self.args)

        self.args.num_params = sum(p.numel() for p in self.mdl.model.parameters())/1e6
        me.args.readonly = False
        me.args.buf = vars(self.args)
        me.args.readonly = True

    def train(self):
        me = self.maester.models[self.args.name]

        print('>>>>>>>start training : {}>>>>>>>>>>>>>>>>>>>>>>>>>>'.format(self.args))
        for i, e, loss, mdl, speed, left_time in self.mdl.train():
            me.status.buf += Entry.Status.finished.running.name
            if DEBUG: print("\titers: {0}, epoch: {1} | loss: {2:.7f}".format(i, e, loss))

        self.args.vali_loss = float(self.mdl.vali_loss)
        me.args.readonly = False
        self.args.buf = vars(self.args)
        me.args.readonly = True
        me.status.buf += Entry.Status.finished.name
