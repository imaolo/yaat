from yaat.util import myprint, filesz, rm, dict2str, str2dict, DEBUG, ROOT, runcmd, filename, path
from yaat.maester import Maester, Entry, Loader

from Informer2020.exp_informer import Exp_Informer
from typing import Any
from tabulate import tabulate
import argparse, sys, torch, numpy as np, pandas as pd, matplotlib.pyplot as plt

def usage():
    # main parser
    main_parser = argparse.ArgumentParser(description='[YAAT] Yet Another Automated Trader')
    main_subparser = main_parser.add_subparsers(dest='cmd', required=True, help='yaat command help')

    # commands
    maester_parser = main_subparser.add_parser(n:='maester', help=f"{n} command help")
    train_parser = main_subparser.add_parser(n:='train', help=f"{n} command help")
    pred_parser = main_subparser.add_parser(n:='pred', help=f"{n} command help")
    scout_parser = main_subparser.add_parser(n:='scout', help=f"{n} command help")
    trade_parser = main_subparser.add_parser(n:='trade', help=f"{n} command help")

    # maester command arguments
    maester_parser.add_argument('--datasets', action='store_const', const='datasets', help="list datasets")
    maester_parser.add_argument('--models', action='store_const', const='models', help="list models.")
    maester_parser.add_argument('--preds', action='store_const', const='preds', help="list predictions.")
    maester_parser.add_argument('--delete', type=str, default=None, help="delete datasets and models with this name")

    # train command arguments
    train_parser.add_argument('--name', type=str, required=True, help='name of the model')
    train_parser.add_argument('--mean_fp', type=str, default='./mean_stuff')
    train_parser.add_argument('--std_fp', type=str, default='./std_stuff')
    train_parser.add_argument('--dataset', type=str, default='cryp2020', help='data')
    train_parser.add_argument('--features', type=str, default='M', help='forecasting task, options:[M, S, MS]; M:multivariate predict multivariate, S:univariate predict univariate, MS:multivariate predict univariate')
    train_parser.add_argument('--target', type=str, default='OT', help='target feature in S or MS task')
    train_parser.add_argument('--freq', type=str, default='h', help='freq for time features encoding, options:[s:secondly, t:minutely, h:hourly, d:daily, b:business days, w:weekly, m:monthly], you can also use more detailed freq like 15min or 3h')
    train_parser.add_argument('--seq_len', type=int, default=1024, help='input sequence length of Informer encoder')
    train_parser.add_argument('--label_len', type=int, default=256, help='start token length of Informer decoder')
    train_parser.add_argument('--pred_len', type=int, default=512, help='prediction sequence length')
    train_parser.add_argument('--enc_in', type=int, default=15, help='encoder input size')
    train_parser.add_argument('--dec_in', type=int, default=15, help='decoder input size')
    train_parser.add_argument('--c_out', type=int, default=15, help='output size')
    train_parser.add_argument('--d_model', type=int, default=1024, help='dimension of model')
    train_parser.add_argument('--n_heads', type=int, default=12, help='num of heads')
    train_parser.add_argument('--e_layers', type=int, default=6, help='num of encoder layers')
    train_parser.add_argument('--d_layers', type=int, default=4, help='num of decoder layers')
    train_parser.add_argument('--s_layers', type=str, default='3,2,1', help='num of stack encoder layers')
    train_parser.add_argument('--d_ff', type=int, default=2048, help='dimension of fcn')
    train_parser.add_argument('--factor', type=int, default=5, help='probsparse attn factor')
    train_parser.add_argument('--padding', type=int, default=0, help='padding type')
    train_parser.add_argument('--distil', action='store_false', help='whether to use distilling in encoder, using this argument means not using distilling', default=True)
    train_parser.add_argument('--dropout', type=float, default=0.2, help='dropout')
    train_parser.add_argument('--attn', type=str, default='prob', help='attention used in encoder, options:[prob, full]')
    train_parser.add_argument('--embed', type=str, default='learned', help='time features encoding, options:[timeF, fixed, learned]')
    train_parser.add_argument('--activation', type=str, default='gelu',help='activation')
    train_parser.add_argument('--output_attention', action='store_true', help='whether to output attention in ecoder')
    train_parser.add_argument('--do_predict', action='store_false', help='whether to predict unseen future data')
    train_parser.add_argument('--mix', action='store_false', help='use mix attention in generative decoder', default=True)
    train_parser.add_argument('--cols', type=str, nargs='+', help='certain cols from the data files as the input features')
    train_parser.add_argument('--num_workers', type=int, default=0, help='data loader num workers')
    train_parser.add_argument('--itr', type=int, default=1, help='experiments times')
    train_parser.add_argument('--train_epochs', type=int, default=10, help='train epochs')
    train_parser.add_argument('--batch_size', type=int, default=32, help='batch size of train input data')
    train_parser.add_argument('--patience', type=int, default=3, help='early stopping patience')
    train_parser.add_argument('--learning_rate', type=float, default=0.0001, help='optimizer learning rate')
    train_parser.add_argument('--des', type=str, default='test',help='exp description')
    train_parser.add_argument('--loss', type=str, default='mse',help='loss function')
    train_parser.add_argument('--lradj', type=str, default='type1',help='adjust learning rate')
    train_parser.add_argument('--use_gpu', type=bool, default=True,help='adjust learning rate')
    train_parser.add_argument('--use_amp', action='store_true', help='use automatic mixed precision training', default=False)
    train_parser.add_argument('--inverse', action='store_true', help='inverse output data', default=False)

    # pred command arguments
    pred_parser.add_argument('--name', type=str, required=True, help='predictions dataset name')
    pred_parser.add_argument('--dataset', type=str, required=True, help='csv path')
    pred_parser.add_argument('--model', type=str, required=True, help='model to use')

    # scout command arguments TODO

    # trade command arguments
    trade_parser.add_argument('--pred', type=str, required=True, help='prediction to visualize')
    
    return main_parser.parse_args()

args = usage()

### driver ###

maester = Maester(ROOT)

if args.cmd == 'maester':

    def print_datasets(): 
        myprint("datasets")
        print(tabulate([[k, filesz(v.dataset.fp), v.status.data[-1], v.cols[0:3]] for k, v in maester.datasets.items()],
                       headers=['name', 'size', 'status', '<=3 cols']))
    def print_models():
        myprint("models")
        print(tabulate([[k,
                         stat:=v.status.data[-1],
                         (a:=str2dict(v.args.data))['num_params'], a['target'], a['features'], a['dataset'],
                         str2dict(v.args.data)['vali_loss'] if stat == Entry.Status.finished.name else None]
                            for k, v in maester.models.items()],
                       headers=['name', 'status', 'num_params', 'target', 'features', 'dataset', 'vali_loss', ]))
    def print_preds():
        myprint("predictions")
        print(tabulate([[k, v.model, v.dataset] for k, v in maester.preds.items()], headers=['name', 'model', 'dataset']))

    if args.delete is not None:
        model = maester.models.pop(args.delete, None)
        if model: rm(model.fp)
        else: print(f"model {args.delete} not found")

        dataset = maester.datasets.pop(args.delete, None)
        if dataset: rm(dataset.fp)
        else: print(f"dataset {args.delete} not found")

    if not args.models and not args.datasets and not args.preds:
        print_datasets(); print_models(); print_preds()
    if args.models is not None: print_models()
    if args.datasets is not None: print_datasets()
    if args.preds is not None: print_preds()

elif args.cmd == 'train':

    assert args.name not in maester.models, f"model {args.name} already exists"
    assert args.dataset in maester.datasets, f"dataset {args.dataset} does not exist"
    maester.create_model(args.name, args=(vars(args) | {'dataset': args.dataset}), model=torch.nn.Linear(1, 1)) # model placeholder

    me = maester.models[args.name]
    de = maester.datasets[args.dataset]

    args.checkpoints = me.weights.fp
    args.root_path = path(*de.dataset.fp.split('/')[:-1])
    args.data_path = filename(de.dataset.fp)
    args.data = 'custom'

    exp = Exp_Informer(args)

    num_params = sum(p.numel() for p in exp.model.parameters())/1e6
    me.args.readonly = False
    me.args.buf = dict2str(str2dict(me.args.data) | {'num_params': f"{num_params}M"})
    me.args.readonly = True

    print('>>>>>>>start training : {}>>>>>>>>>>>>>>>>>>>>>>>>>>'.format(args))
    for i, e, loss, mdl, speed, left_time in exp.train():
        me.status.buf += Entry.Status.finished.running.name
        if DEBUG: print("\titers: {0}, epoch: {1} | loss: {2:.7f}".format(i, e, loss))

    me.args.readonly = False
    me.args.buf = dict2str(str2dict(me.args.data) | {'vali_loss': float(exp.vali_loss)})
    me.args.readonly = True
    me.status.buf += Entry.Status.finished.name

elif args.cmd == 'pred':

    assert args.name not in maester.preds, f"prediction dataset {args.name} already exists"
    assert args.model in maester.models, f"model {args.model} does not exists"
    assert args.dataset in maester.datasets, f"dataset {args.dataset} does not exist"
    maester.create_pred(args.name, np.array(1), args.model, args.dataset)

    me = maester.models[args.model]
    de = maester.datasets[args.dataset]
    pe = maester.preds[args.name]

    args.checkpoints = me.weights.fp
    args.root_path = path(*de.dataset.fp.split('/')[:-1])
    args.data_path = filename(de.dataset.fp)
    args.data = 'custom'
    args.pred_fp = pe.pred.fp
    for k, v in str2dict(me.args.data).items(): setattr(args, k, v)
    args.detail_freq = args.freq
    args.inverse = True

    print('>>>>>>>predicting : {}<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'.format(args))
    Exp_Informer(args).predict(True)

elif args.cmd == 'scout': pass # TODO

elif args.cmd == 'trade':

    assert args.pred in maester.preds

    y = maester.preds[args.pred].pred.data.squeeze(0)
    x = np.arange(len(y))

    # Create subplots - 15 rows, 1 column
    fig, axs = plt.subplots(y.shape[1], 1, figsize=(10, 20))

    for i in range(y.shape[1]):  # Loop through each of the 15 lines
        axs[i].plot(y[:, i], label=f'Line {i+1}')
        axs[i].legend()
        axs[i].set_xlabel('X-axis')
        axs[i].set_ylabel('Y-axis')

    plt.tight_layout()  # Adjust layout to not overlap subplots
    plt.show()

else: raise RuntimeError(f"invalid command {args.cmd}")