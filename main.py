from yaat.maester import Maester
from dataclasses import asdict
from pprint import pprint
from datetime import datetime
from yaat.informer import Informer, InformerArgs
import argparse, io, torch, pandas as pd

# main parser
main_parser = argparse.ArgumentParser(description='[YAAT] Yet Another Automated Trader')

# main arguments
main_parser.add_argument('--dbdir', type=str, default=None, help='which directory to start local db')
main_parser.add_argument('--connstr', type=str, default=None, help='database connection string')

# sub - commands
main_subparser = main_parser.add_subparsers(dest='cmd', required=True, help='yaat command help')
maester_parser = main_subparser.add_parser(n:='maester', help=f"{n} command help")
train_parser = main_subparser.add_parser(n:='train', help=f"{n} command help")
predict_parser = main_subparser.add_parser(n:='predict', help=f"{n} command help")

# maester command arguments

maester_parser.add_argument('--describe_tickers', nargs='+', type=str, default=None)
maester_parser.add_argument('--coll', type=str, default='candles1min', help='which collection to look in (doesnt apply to all flags)')
maester_parser.add_argument('--list_tickers', action='store_true', default=False, help='list unique tickers')
maester_parser.add_argument('--list_data_colls', action='store_true', default=False, help='list data collections')
maester_parser.add_argument('--list_models', action='store_true', default=False, help='list data collections')
maester_parser.add_argument('--delete_models', nargs='+', type=str, default=None)
maester_parser.add_argument('--list_tickers_by_counts', type=int, default=None, help='list the top N most occuring tickers')

# predict command arguments
predict_parser.add_argument('--model_name', type=str, required=True)
predict_parser.add_argument('--start_date', type=str, default=datetime.now().strftime('%Y-%m-%d'))

# train command arguments


train_parser.add_argument('--just_open', action='store_true', default=False)

train_parser.add_argument('--tickers', type=str, required=True, nargs='+', help='ticker symbols to train on')

train_parser.add_argument('--name', type=str, required=True, help='name of this model, for human readability')

train_parser.add_argument('--model', type=str, default='informer',help='model of experiment, options: [informer, informerstack, informerlight(TBD)]')

train_parser.add_argument('--data', type=str, default='custom', help='data')
train_parser.add_argument('--features', type=str, default='M', help='forecasting task, options:[M, S, MS]; M:multivariate predict multivariate, S:univariate predict univariate, MS:multivariate predict univariate')
train_parser.add_argument('--target', type=str, default='OT', help='target feature in S or MS task')
train_parser.add_argument('--freq', type=str, default='h', help='freq for time features encoding, options:[s:secondly, t:minutely, h:hourly, d:daily, b:business days, w:weekly, m:monthly], you can also use more detailed freq like 15min or 3h')
train_parser.add_argument('--checkpoints', type=str, default='./checkpoints/', help='location of model checkpoints')

train_parser.add_argument('--seq_len', type=int, default=96, help='input sequence length of Informer encoder')
train_parser.add_argument('--label_len', type=int, default=48, help='start token length of Informer decoder')
train_parser.add_argument('--pred_len', type=int, default=24, help='prediction sequence length')
# Informer decoder input: concat[start token series(label_len), zero padding series(pred_len)]

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
    # TODO - deploy on gpu instance if specified'

    assert args.features != 'M', 'unsupported'

    # get the dataset
    print(f"retrieving the dataset for {args.tickers}")
    dataset_size, dataset_path = maester.get_dataset(args.tickers)
    print("dataset size: ", dataset_size)
    print("dataset path: ", dataset_path)

    if args.just_open:
        df = pd.read_csv(dataset_path)
        df.drop([col for col in df.columns if '_open' not in col and col != 'date'], axis=1, inplace=True)
        df.to_csv(dataset_path)
        print(df.columns)

    # set model parameters that are dependenent on the dataset
    args.enc_in = args.dec_in = len(pd.read_csv(dataset_path).columns)-1
    args.cout = 1

    # get the args (remove those not in InformerArgs)
    train_arg_names = [action.dest for action in train_parser._actions] + ['enc_in', 'dec_in' , 'c_out']
    train_args = {k: v for k, v in vars(args).items() if k in train_arg_names and k not in ['tickers', 'name', 'just_open']}
    informer_args = InformerArgs(**(train_args | {'root_path': str(dataset_path.parent), 'data_path': str(dataset_path.name)}))
    informer_args_dict = asdict(informer_args)

    # create the model
    informer = Informer(informer_args)

    # store the model
    maester.insert_informer(args.name, args.tickers, informer)

    # train the model
    print("training model"); informer.train(); print("training complete")

    # store the new weights
    maester.set_informer_weights(informer)

    # test the model
    print("testing model"); informer.test(); print("testing complete")

    # store the test's results
    maester.set_informer_loss(informer)

elif args.cmd == 'predict':

    # get the informer document
    model_doc = maester.informer_weights.find_one({'name': args.model_name})
    if model_doc is None: raise RuntimeError(f"model name {args.model_name} dne")

    # get the prediction data
    last_date, dataset_path = maester.get_prediction_data(args.start_date, model_doc)
    print("last prediction data date: ", last_date)
    print("prediction data path: ", dataset_path)

    # get the args
    train_arg_names = [action.dest for action in train_parser._actions]
    train_args = {k: v for k, v in model_doc.items() if k in train_arg_names and k not in ['tickers', 'name']}
    informer_args = InformerArgs(**(train_args | {'root_path': str(dataset_path.parent), 'data_path': str(dataset_path.name)}))
    informer_args_dict = asdict(informer_args)

    # create the model
    informer = Informer(informer_args)

    # get the weights file
    weights_file = maester.fs.get(model_doc['weights_file_id'])

    # get the bytes
    state_dict_bytes = weights_file.read()

    # load the bytes into the model
    with io.BytesIO(state_dict_bytes) as bytes_io:
        state_dict_deser = torch.load(bytes_io)
    informer.exp_model.model.load_state_dict(state_dict_deser)

    # do prediction
    informer.predict()

    # store predictions
    maester.store_predictions(args.model_name, last_date, informer.predictions_file_path)


elif args.cmd == 'maester':

    if args.describe_tickers is not None:
        # TODO - print count, start_date, and end_date
        for tick in args.describe_tickers:
            print(tick, maester.db[args.coll].count_documents({'ticker': tick}))

    if args.list_tickers:
        pprint(list(maester.db[args.coll].distinct('ticker')), compact=True)

    if args.list_data_colls:
        pprint(maester.data_collections)

    if args.list_tickers_by_counts is not None:
        ticker_counts = list(maester.db[args.coll].aggregate([
            {'$group': {
                '_id': '$ticker',
                'count': {'$sum': 1},
            }},
            {'$sort': {'count': -1}},
            {'$project': {
                'ticker': '$_id',
                'count': 1,
                '_id': 0
            }},
            {'$limit': args.list_tickers_by_counts}
        ]))
        pprint(ticker_counts)

    if args.list_models:
        model_docs = list(maester.informer_weights.find({}, {'name': 1, 'mse': 1, 'tickers': 1, 'timestamp': 1, 'weights_file_id': 1, '_id': 0}).sort('mse', 1))
        pprint(model_docs)

    if args.delete_models is not None:
        for mod in args.delete_models:
            res = maester.informer_weights.delete_one({'name': mod})
            if res.deleted_count > 0: print(f"deleted model {mod}")
            else: print(f"model {mod} dne")



# TODO - train