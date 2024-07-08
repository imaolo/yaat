from yaat.maester import Maester
import argparse

# main parser
main_parser = argparse.ArgumentParser(description='[YAAT] Yet Another Automated Trader')

# main arguments
main_parser.add_argument('--dbdir', type=str, default=None, help='which directory to start local db')
main_parser.add_argument('--connstr', type=str, default=None, help='database connection string')

# sub - commands
main_subparser = main_parser.add_subparsers(dest='cmd', required=True, help='yaat command help')
maester_parser = main_subparser.add_parser(n:='maester', help=f"{n} command help")
train_parser = main_subparser.add_parser(n:='train', help=f"{n} command help")

# The maester command just prints the models

# train command arguments
train_parser.add_argument('--model', type=str, default='informer',help='model of experiment, options: [informer, informerstack, informerlight(TBD)]')

train_parser.add_argument('--data', type=str, default='custom', help='data')
train_parser.add_argument('--root_path', type=str, default='./data/ETT/', help='root path of the data file')
train_parser.add_argument('--data_path', type=str, default='ETTh1.csv', help='data file')    
train_parser.add_argument('--features', type=str, default='M', help='forecasting task, options:[M, S, MS]; M:multivariate predict multivariate, S:univariate predict univariate, MS:multivariate predict univariate')
train_parser.add_argument('--target', type=str, default='OT', help='target feature in S or MS task')
train_parser.add_argument('--freq', type=str, default='h', help='freq for time features encoding, options:[s:secondly, t:minutely, h:hourly, d:daily, b:business days, w:weekly, m:monthly], you can also use more detailed freq like 15min or 3h')
train_parser.add_argument('--checkpoints', type=str, default='./checkpoints/', help='location of model checkpoints')

train_parser.add_argument('--seq_len', type=int, default=96, help='input sequence length of Informer encoder')
train_parser.add_argument('--label_len', type=int, default=48, help='start token length of Informer decoder')
train_parser.add_argument('--pred_len', type=int, default=24, help='prediction sequence length')
# Informer decoder input: concat[start token series(label_len), zero padding series(pred_len)]

train_parser.add_argument('--enc_in', type=int, default=7, help='encoder input size')
train_parser.add_argument('--dec_in', type=int, default=7, help='decoder input size')
train_parser.add_argument('--c_out', type=int, default=7, help='output size')
train_parser.add_argument('--d_model', type=int, default=512, help='dimension of model')
train_parser.add_argument('--n_heads', type=int, default=8, help='num of heads')
train_parser.add_argument('--e_layers', type=int, default=2, help='num of encoder layers')
train_parser.add_argument('--d_layers', type=int, default=1, help='num of decoder layers')
train_parser.add_argument('--s_layers', type=str, default='3,2,1', help='num of stack encoder layers')
train_parser.add_argument('--d_ff', type=int, default=2048, help='dimension of fcn')
train_parser.add_argument('--factor', type=int, default=5, help='probsparse attn factor')
train_parser.add_argument('--padding', type=int, default=0, help='padding type')
train_parser.add_argument('--distil', action='store_false', help='whether to use distilling in encoder, using this argument means not using distilling', default=True)
train_parser.add_argument('--dropout', type=float, default=0.05, help='dropout')
train_parser.add_argument('--attn', type=str, default='prob', help='attention used in encoder, options:[prob, full]')
train_parser.add_argument('--embed', type=str, default='timeF', help='time features encoding, options:[timeF, fixed, learned]')
train_parser.add_argument('--activation', type=str, default='gelu',help='activation')
train_parser.add_argument('--output_attention', action='store_true', help='whether to output attention in ecoder')
train_parser.add_argument('--mix', action='store_false', help='use mix attention in generative decoder', default=True)
train_parser.add_argument('--cols', type=str, nargs='+', help='certain cols from the data files as the input features')
train_parser.add_argument('--num_workers', type=int, default=0, help='data loader num workers')
train_parser.add_argument('--train_epochs', type=int, default=6, help='train epochs')
train_parser.add_argument('--batch_size', type=int, default=32, help='batch size of train input data')
train_parser.add_argument('--patience', type=int, default=3, help='early stopping patience')
train_parser.add_argument('--learning_rate', type=float, default=0.0001, help='optimizer learning rate')
train_parser.add_argument('--des', type=str, default='test',help='exp description')
train_parser.add_argument('--loss', type=str, default='mse',help='loss function')
train_parser.add_argument('--lradj', type=str, default='type1',help='adjust learning rate')
train_parser.add_argument('--use_amp', action='store_true', help='use automatic mixed precision training', default=False)
train_parser.add_argument('--inverse', action='store_true', help='inverse output data', default=False)

train_parser.add_argument('--use_gpu', type=bool, default=True, help='use gpu')
train_parser.add_argument('--gpu', type=int, default=0, help='gpu')
train_parser.add_argument('--use_multi_gpu', action='store_true', help='use multiple gpus', default=False)
train_parser.add_argument('--devices', type=str, default='0,1,2,3',help='device ids of multile gpus')

args = main_parser.parse_args()

maester = Maester(connstr=args.connstr, dbdir=args.dbdir)

if args.cmd == 'train':
    # TODO - deploy on gpu instance if specified

    from yaat.informer import Informer

    # check if the model exists in the database already
    # - specs
    # - data trained on
    # TODO

    # get train sub-command arguments
    train_args = [action.dest for action in train_parser._actions]

    # create the model (filter out non-train sub-command arguments)
    informer = Informer(**{k: v for k, v in vars(args).items() if k in train_args})

    # train the model
    informer.train()
    
    # store the model in the database
    # TODO

elif args.cmd == 'maester':
    pass # TODO - list the models available

    