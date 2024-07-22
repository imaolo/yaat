from datetime import timedelta
from yaat.main import parse_args, train, predict, maester
from yaat.maester import PredictionDoc, InformerDoc
from datetime import datetime
import pandas as pd, numpy as np, matplotlib.pyplot as plt

args = parse_args()

if args.cmd == 'train':
    train(args)

elif args.cmd == 'predict':
    predict(args)

elif args.cmd == 'plot_prediction':

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

# TODO - scale down the model and try a bunch of combinations to see what works best at predicting
# TODO - predictions are more than just the end of the day
# TODO - get better schema update mechanism because I just frigged at 6 hour epoch
# TODO - true batch scaling, not instance scaling
# TODO - handle live jobs better, record them in the database
# TODO - test get_live_data
