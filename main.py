from yaat.maester import Maester
from dataclasses import asdict
from pprint import pprint
from datetime import datetime, timedelta
from yaat.informer import Informer, InformerArgs
from yaat.main import parse_args
import argparse, io, torch, pandas as pd, numpy as np, matplotlib.pyplot as plt

args = parse_args()

import sys
sys.exit()

if args.connstr == 'None': args.connstr = None
maester = Maester(connstr=args.connstr, dbdir=args.dbdir)



if args.cmd == 'train':
    # TODO - deploy on gpu instance if specified'

    assert args.features != 'M', 'unsupported'

    # get the dataset
    print(f"retrieving the dataset for {args.tickers}")
    dataset_size, dataset_path, dataset_fields = maester.get_dataset(args.tickers, args.fields, args.max_data, args.alpha_dataset)
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
    maester.insert_informer(args.name, args.tickers, informer, dataset_fields, args.alpha_dataset)

    # train the model
    print("training model")
    print(args)
    for idx, update in enumerate(informer.exp_model.train(informer.settings)):
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

    # get the args - TODO - get ride of mean and std for real
    informer_args = InformerArgs.from_dict(model_doc
                                            |   {'root_path': str(dataset_path.parent)}
                                            |   {'data_path': str(dataset_path.name)}
                                            |   {'std': None}
                                            |   {'mean': None})

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
# TODO - better alpha dataset support
# TODO - predictions are more than just the end of the day
# TODO - get better schema update mechanism because I just frigged at 6 hour epoch
# TODO - true batch scaling, not instance scaling
# TODO - hotfix for predicting when batch scaled (self.scaler is never fitted)
# TODO - use alphavantage, that inclues collections per ticker rather than just one collection.
# make a db just for candles
# TODO - combine get_dataset and get_prediction_dataset
# TODO - change "batch_scale" argument name to "sample_scale
# TODO - handle live jobs better, record them in the database
# TODO - create dataclasses for each of the collection, use the pymmap to read them in
# TODO - clean things up!! (overload informer_args for the informer doc)
