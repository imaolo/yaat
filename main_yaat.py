# from dropbox import Dropbox
import argparse, torch #, uuid, os, numpy as np

class ArgParser:
    main_parser = argparse.ArgumentParser(description='[YAAT] Yet Another Automated Trader')
    main_subparser = main_parser.add_subparsers(dest='cmd', required=True, help='Sub-command help')

    # informer command
    informer_parser = main_subparser.add_parser('informer', help='informer sub-command help')

    # informer subcommands
    informer_subparser = informer_parser.add_subparsers(dest='informer_cmd', help='yaat informer help', required=True)
    train = informer_subparser.add_parser(name='train', help='yaat train help')
    eval = informer_subparser.add_parser(name='eval', help='yaat evaluation help')
    pred = informer_subparser.add_parser(name='pred', help='yaat prediction help')

    # trader command
    trader_parser = main_subparser.add_parser('trader', help='trader sub-command help')

    # trader subcommands
    trader_subparser = trader_parser.add_subparsers(dest='trader_cmd', help='yaat trader help', required=True)
    trader = trader_subparser.add_parser('trade', help='yaat trade help')

    @classmethod
    def parseArgs(self):
        for func in [self.addInformerArgs, self.addTraderArgs]: func()
        args = self.main_parser.parse_args()
        if args.cmd == 'informer': 
            args.use_gpu = True if torch.cuda.is_available() and args.informer.use_gpu else False
        return args

    # add informer command (and subcommand) arguments
    @classmethod
    def addInformerArgs(self):
        # these arguments are shared by all sub commands

        # env
        self.informer_parser.add_argument('--root_path', type=str, default='./data', help='root path of the data file')
        self.informer_parser.add_argument('--data_path', type=str, default='tickers.csv', help='data file')
        self.informer_parser.add_argument('--checkpoints', type=str, default='./checkpoints/', help='location of model checkpoints')

        # define model
        self.informer_parser.add_argument('--features', type=str, default='M', help='forecasting task, options:[M, S, MS]; M:multivariate predict multivariate, S:univariate predict univariate, MS:multivariate predict univariate')
        self.informer_parser.add_argument('--target', type=str, default='OT', help='target feature in S or MS task')
        self.informer_parser.add_argument('--freq', type=str, default='h', help='freq for time features encoding, options:[s:secondly, t:minutely, h:hourly, d:daily, b:business days, w:weekly, m:monthly], you can also use more detailed freq like 15min or 3h')
        self.informer_parser.add_argument('--seq_len', type=int, default=96, help='input sequence length of Informer encoder')
        self.informer_parser.add_argument('--label_len', type=int, default=48, help='start token length of Informer decoder')
        self.informer_parser.add_argument('--pred_len', type=int, default=24, help='prediction sequence length')
        self.informer_parser.add_argument('--enc_in', type=int, default=7, help='encoder input size')
        self.informer_parser.add_argument('--dec_in', type=int, default=7, help='decoder input size')
        self.informer_parser.add_argument('--c_out', type=int, default=7, help='output size')
        self.informer_parser.add_argument('--d_model', type=int, default=512, help='dimension of model')
        self.informer_parser.add_argument('--n_heads', type=int, default=8, help='num of heads')
        self.informer_parser.add_argument('--e_layers', type=int, default=2, help='num of encoder layers')
        self.informer_parser.add_argument('--d_layers', type=int, default=1, help='num of decoder layers')
        self.informer_parser.add_argument('--s_layers', type=str, default='3,2,1', help='num of stack encoder layers')
        self.informer_parser.add_argument('--d_ff', type=int, default=2048, help='dimension of fcn')
        self.informer_parser.add_argument('--factor', type=int, default=5, help='probsparse attn factor')
        self.informer_parser.add_argument('--padding', type=int, default=0, help='padding type')
        self.informer_parser.add_argument('--distil', action='store_false', help='whether to use distilling in encoder, using this argument means not using distilling', default=True)
        self.informer_parser.add_argument('--dropout', type=float, default=0.05, help='dropout')
        self.informer_parser.add_argument('--attn', type=str, default='prob', help='attention used in encoder, options:[prob, full]')
        self.informer_parser.add_argument('--embed', type=str, default='timeF', help='time features encoding, options:[timeF, fixed, learned]')
        self.informer_parser.add_argument('--activation', type=str, default='gelu',help='activation')
        self.informer_parser.add_argument('--output_attention', action='store_true', help='whether to output attention in ecoder')
        self.informer_parser.add_argument('--mix', action='store_false', help='use mix attention in generative decoder', default=True)
        self.informer_parser.add_argument('--cols', type=str, nargs='+', help='certain cols from the data files as the input features')
        self.informer_parser.add_argument('--learning_rate', type=float, default=0.0001, help='optimizer learning rate')
        self.informer_parser.add_argument('--des', type=str, default='test',help='exp description')
        self.informer_parser.add_argument('--loss', type=str, default='mse',help='loss function')
        self.informer_parser.add_argument('--lradj', type=str, default='type1',help='adjust learning rate')

        # operation
        self.informer_parser.add_argument('--do_predict', action='store_true', help='whether to predict unseen future data')
        self.informer_parser.add_argument('--num_workers', type=int, default=0, help='data loader num workers')
        self.informer_parser.add_argument('--itr', type=int, default=2, help='experiments times')
        self.informer_parser.add_argument('--train_epochs', type=int, default=6, help='train epochs')
        self.informer_parser.add_argument('--batch_size', type=int, default=32, help='batch size of train input data')
        self.informer_parser.add_argument('--patience', type=int, default=3, help='early stopping patience')
        self.informer_parser.add_argument('--use_gpu', type=bool, default=True, help='use gpu')
        self.informer_parser.add_argument('--gpu', type=int, default=0, help='gpu')
        self.informer_parser.add_argument('--use_multi_gpu', action='store_true', help='use multiple gpus', default=False)
        self.informer_parser.add_argument('--devices', type=str, default='0,1,2,3',help='device ids of multiple gpus')
        self.informer_parser.add_argument('--inverse', action='store_true', help='inverse output data', default=False)
        self.informer_parser.add_argument('--lambda_labs', action='store_true', help='deploy on lambda labs', default=True)
        self.informer_parser.add_argument('--dropbox', action='store_true', help='deploy on lambda labs', default=True)

        # TODO these arguments are sub command specific
        def addTrainArgs(): pass
        def addEvalArgs(): pass
        def addPredArgs(): pass
        for func in [addTrainArgs, addEvalArgs, addPredArgs]: func()

    # TODO add trader arguments
    @classmethod
    def addTraderArgs(self): pass

args = ArgParser.parseArgs()

# TODO
# if specified, startup, connect, dispatch all commands to gpu instance
# download data
# run model & log status
# upload results

# code graveyard

# # helpers
# def create_dir(path, dbx=None, can_exist=True):
#     if can_exist:
#         if dbx and any(file_entry.name == path.split('/')[-1] for file_entry in dbx.files_list_folder('').entries):
#             return
#         if not dbx and os.path.isdir(path):
#             return
#     if dbx: dbx.files_create_folder(path)
#     else: os.mkdir(path)
# # TODO - download upload - from and to paths

# def dl(fro_p:str, to_p:str, dbx): pass
# def ul(fro_p:str, to_p:str, dbx): pass

# dbx = Dropbox('sl.BwwHSuAQqUBMp2d32i5rmzrlDnKMNSVSSmsCHnWhMmavmXPd0AcQxfy42QIeyUDJWbwSRO3IqEzK_kFCV3UWcAMhPhTShwlQM6jZM-0KLQnUctpQ42pXYVRRjWmlOsXumOczW4sHZjP9')    
# create_dir('/jobs', dbx)
# create_dir(job_dir:='/jobs/'+str(uuid.uuid4()), dbx)