from typing import Optional, Dict
from datetime import datetime, timedelta
from yaat.maester import Maester, InformerDoc, PredictionDoc
from yaat.informer import Informer
from yaat.util import getenv
from dataclasses import asdict
from pathlib import Path
from bson import Int64
import argparse, inspect, tempfile, os, numpy as np, pandas as pd, matplotlib.pyplot as plt

DB_UA, DB_PW, DB_IP, DB_DIR = getenv('DB_UA', None), getenv('DB_PW', None), getenv('DB_IP', None), getenv('DB_DIR', None)

# main parser
main_parser = argparse.ArgumentParser(description='[YAAT] Yet Another Automated Trader')

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
train_parser.add_argument('--start_date', type=str, default=None)
train_parser.add_argument('--end_date', type=str, default=None)
train_parser.add_argument('--sample_scale', action='store_true', default=False)
train_parser.add_argument('--fields', type=str, default=None, nargs='+', help='ticker fields to train on')
train_parser.add_argument('--tickers', type=str, required=True, nargs='+', help='ticker symbols to train on')

train_parser.add_argument('--name', type=str, required=True, help='name of this model, for human readability')

train_parser.add_argument('--model', type=str, default='informer',help='model of experiment, options: [informer, informerstack, informerlight(TBD)]')

train_parser.add_argument('--target', type=str, required=True, help='target feature in S or MS task')
train_parser.add_argument('--freq', type=str, default='t', help='freq for time features encoding, options:[s:secondly, t:minutely, h:hourly, d:daily, b:business days, w:weekly, m:monthly], you can also use more detailed freq like 15min or 3h')
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
train_parser.add_argument('--embed', type=str, default='learned', help='time features encoding, options:[timeF, fixed, learned]')
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

def parse_args(cmd:Optional[str]=None, args: Dict[str, str]=None):
    _args = None
    if cmd is not None:
        assert args is not None
        _args = [cmd]
        for k, v in args.items():
            _args.append('--'+k)
            if v is not None:
                _args.append(v)
    return main_parser.parse_args(_args)

assert DB_PW is None == DB_IP is None == DB_UA is None, f"{DB_UA} - {DB_PW} - {DB_IP}"
maester = Maester(connstr= None if DB_PW is None else f'mongodb://{DB_UA}:{DB_PW}@{DB_IP}/', dbdir=DB_DIR)

def train(args):
    # TODO - deploy on gpu instance if specified'
    assert args.cmd == inspect.currentframe().f_code.co_name, args.cmd
    print(args)

    # get the start and end dates
    if args.start_date is not None: args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    if args.end_date is not None: args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

    # get the dataset
    print(f"retrieving dataset: tickers - {args.tickers}, fields - {args.fields}")
    df = maester.get_dataset(args.tickers, args.fields, start_date=args.start_date, end_date=args.end_date)

    # save to file
    df_fp = Path(tempfile.NamedTemporaryFile(delete=False, suffix='.csv').name)
    df.to_csv(df_fp)

    # create the doc
    informer_doc: InformerDoc = InformerDoc.from_dict(vars(args)
        | {'root_path': str(df_fp.parent)}
        | {'data_path': str(df_fp.name)}
        | {'fields': list(set(col.split('_')[1] for col in df.columns if col != 'date'))})

    # create the model
    informer = Informer(informer_doc)

    # set num_params
    informer_doc.num_params = Int64(informer.num_params)

    # insert the document
    maester.informers.insert_one(asdict(informer_doc))

    # train the model
    print("training model")
    for update in informer.train():
        maester.informers.update_one({'name': args.name}, {'$set': update})
        if 'test_loss' in update.keys():
            maester.set_informer_weights(informer_doc.name, informer)
    print("training complete")

def predict(args):
    assert args.cmd == inspect.currentframe().f_code.co_name, args.cmd
    print(args)

    # get the informer doc
    informer_doc = InformerDoc(**maester.informers.find_one({'name': args.model_name}, {'_id': 0}))

    # get start and end dates TODO - fix start->end date
    end_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    start_date = end_date - timedelta(days=1)

    # get the dataset
    df = maester.get_live_data(informer_doc.tickers, informer_doc.fields, start_date=start_date, end_date=end_date)

    # save to file
    df_fp = Path(tempfile.NamedTemporaryFile(delete=False, suffix='.csv').name)
    df.to_csv(df_fp)

    # set the root and data path
    informer_doc.root_path = str(df_fp.parent)
    informer_doc.data_path = str(df_fp.name)

    # create the model
    informer = Informer(informer_doc)

    # load the model
    informer.load_weights(maester.fs.get(informer_doc.weights_file_id).read())

    # predict
    informer.predict()

    # store predictions
    maester.predictions.insert_one(asdict(PredictionDoc(args.name, args.model_name, pd.to_datetime(df['date'].max()),
                                                        np.load(informer.predictions_file_path).flatten().tolist())))

def plot_prediction(args):

    # get prediction doc
    pred_doc = PredictionDoc(**maester.predictions.find_one({'name': args.name}, {'_id': 0}))
    
    # get model doc
    model_doc = InformerDoc(**maester.informers.find_one({'name': pred_doc.model_name}, {'_id': 0}))

    # get the prediction data
    preds = np.array(pred_doc.predictions).squeeze()
    
    # get the real data
    target_ticker = model_doc.target.split('_')[0]
    target_field = model_doc.target.split('_')[1]
    real_df = maester.get_live_data([target_ticker], [target_field], pred_doc.pred_date, pred_doc.pred_date + timedelta(days=4))
    real_df = real_df.head(model_doc.pred_len)

    # place predictions in dataframe
    real_df['preds'] = preds

    # Plotting
    target_field_full = f"{target_ticker}_{target_field}"
    plt.figure(figsize=(10, 6))
    plt.plot(real_df.index, real_df[target_field_full], label=target_field_full, marker='o')
    plt.plot(real_df.index, real_df['preds'], label='preds', marker='x')
    plt.xlabel('Index')
    plt.ylabel('Value')
    plt.title(target_field_full)
    plt.legend()
    plt.grid(True)
    plt.show()