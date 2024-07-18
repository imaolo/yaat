from yaat.maester import Maester
from dataclasses import asdict
from pprint import pprint
from datetime import datetime, timedelta
from yaat.informer import Informer, InformerArgs
import argparse, io, torch, pandas as pd, numpy as np, matplotlib.pyplot as plt

# main parser
main_parser = argparse.ArgumentParser(description='[YAAT] Yet Another Automated Trader')

# main arguments
main_parser.add_argument('--dbdir', type=str, default=None, help='which directory to start local db')
main_parser.add_argument('--connstr', type=str, default='mongodb://Earl:pink-Flamingo1317@52.91.137.11/', help='database connection string')

# sub - commands
main_subparser = main_parser.add_subparsers(dest='cmd', required=True, help='yaat command help')
train_parser = main_subparser.add_parser(n:='train', help=f"{n} command help")
predict_parser = main_subparser.add_parser(n:='predict', help=f"{n} command help")
plot_prediction_parser = main_subparser.add_parser(n:='plot_prediction', help=f"{n} command help")

# predict command arguments
predict_parser.add_argument('--name', type=str, required=True)
predict_parser.add_argument('--model_name', type=str, required=True)
predict_parser.add_argument('--start_date', type=str, default=datetime.now().strftime('%Y-%m-%d'))

# plot prediction command arguments
plot_prediction_parser.add_argument('--name', type=str, required=True)

# train command arguments


train_parser.add_argument('--max_data', type=int, default=None, help='max datapoints to train')
train_parser.add_argument('--fields', type=str, default=None, nargs='+', help='ticker fields to train on')
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
    dataset_size, dataset_path, dataset_fields = maester.get_dataset(args.tickers, args.fields, args.max_data)
    print("dataset size: ", dataset_size)
    print("dataset path: ", dataset_path)

    # set model parameters that are dependenent on the dataset
    args.enc_in = args.dec_in = len(pd.read_csv(dataset_path).columns)-1
    args.c_out = 1

    # get the args
    informer_args = InformerArgs.from_dict(vars(args) | {'root_path': str(dataset_path.parent), 'data_path': str(dataset_path.name)})

    # create the model
    informer = Informer(informer_args)

    # store the model
    maester.insert_informer(args.name, args.tickers, informer, dataset_fields)

    # train the model
    print("training model")
    for idx, update in enumerate(informer.exp_model.train(informer.settings)):
        # store training set mean and std
        if idx == 0:
            maester.informer_weights.update_one({'name': args.name}, {'$set': {
                'std': list(informer.exp_model.train_data.scaler.std),
                'mean': list(informer.exp_model.train_data.scaler.mean)
            }})
        # update stats
        maester.informer_weights.update_one({'name': args.name}, {'$set': update})
        # check point
        if 'test_loss' in update.keys(): maester.set_informer_weights(informer)
    print("training complete")

    # store the new weights
    maester.set_informer_weights(informer)

elif args.cmd == 'predict':

    # get the informer document
    model_doc = maester.informer_weights.find_one({'name': args.model_name})
    if model_doc is None: raise RuntimeError(f"model name {args.model_name} dne")

    # get the prediction data
    last_date, dataset_path = maester.get_prediction_data(args.start_date, model_doc)
    print("last prediction data date: ", last_date)
    print("prediction data path: ", dataset_path)

    # get the args
    informer_args = InformerArgs.from_dict(model_doc
                                            |   {'root_path': str(dataset_path.parent)}
                                            |   {'data_path': str(dataset_path.name)}
                                            |   {'std': np.array(model_doc['std'])}
                                            |   {'mean': np.array(model_doc['mean'])})

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
    informer.exp_model.predict(informer.settings)

    # store predictions
    maester.store_predictions(args.name, args.model_name, last_date, informer.predictions_file_path)

elif args.cmd == 'plot_prediction':

    # get prediction doc
    pred_doc = list(maester.predictions.find({'name': args.name}))
    assert len(pred_doc) == 1,f"only one prediction document allowed for {args.name}, {len(pred_doc)} found"
    pred_doc = pred_doc[0]
    
    # get model doc
    model_name = pred_doc['model_name']
    model_doc = list(maester.informer_weights.find({'name': model_name}))
    assert len(model_doc) == 1,f"only one model document allowed for {model_name}, {len(model_doc)} found"
    model_doc = model_doc[0]

    # get the prediction data
    preds = np.array(pred_doc['predictions']).squeeze()

    start_pred_date = pred_doc['last_date'] + timedelta(days=1)
    start_pred_date = start_pred_date.strftime('%Y-%m-%d')

    last_date_actual, actual_dataset_path = maester.get_prediction_data(start_pred_date, model_doc)

    actual_df = pd.read_csv(actual_dataset_path)
    actual_df['timestamp'] = pd.to_datetime(actual_df['date'], format='%Y-%m-%d %H:%M:%S')

    print(actual_df['timestamp'].min())

    actual_df = actual_df.drop([col for col in actual_df.columns if '_open' not in col], axis=1).head(model_doc['pred_len'])

    actual_df['preds'] = preds
    # actual_df['preds'] = actual_df['preds'].rolling(window=5).mean()

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.plot(actual_df.index, actual_df['SPY_open'], label='SPY_open', marker='o')
    plt.plot(actual_df.index, actual_df['preds'], label='preds', marker='x')
    plt.xlabel('Index')
    plt.ylabel('Value')
    plt.title('SPY_open and preds')
    plt.legend()
    plt.grid(True)
    plt.show()

# TODO - scale down the model and try a bunch of combinations to see what works best at predicting
# TODO - refactor for testing
# def parse_args(args=None):
#     # Use main_parser defined at the top level of this module
#     return main_parser.parse_args(args)
# TODO - when retrieving datasets, filter fields on server, not client
