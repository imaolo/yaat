from yaat.maester import Maester
from dataclasses import asdict
from pprint import pprint
from datetime import datetime, timedelta
from yaat.informer import Informer, InformerArgs
from yaat.main import parse_args, train, predict, maester
import argparse, io, torch, pandas as pd, numpy as np, matplotlib.pyplot as plt

args = parse_args()

if args.cmd == 'train':
    train(args)

elif args.cmd == 'predict':
    predict(args)

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
