# Yaat - Yet Another Automated Trader
## install
```sh
python -m pip install -e . 
```

## testing
```sh
python -m pip install -e '.[testing]'
python -m pytest test/ 
```

## usage

The maester provides information on available datasets, models, and predictions
We can can print all, some, or one of them.
It also downloads seed data if there is not existing data
```sh
python main maester
python main maester --models
python main maester --datasets
python main maester --preds
python main maester --preds --datasets
```

The seed datasets should be listed under datasets.


Use the cryp2020 dataset to train a model.

You will want to go in the data directory and cut down the size of the csv, otherwise training will take forever. It is not hard to navigate and is the same information presented by the maester. This will create extraneous .npy files in pwd. They can be ignored and deleted.

```sh
python main train --name mymodelname --dataset cryp2020
python main maester --models
```

Use the model to make predictions.

```sh
python main maester --models --datasets
python main pred --name mypred --dataset cryp2020 --model mymodelname
python main maester --preds
```

Visualize the predictions
```sh
python main trade --pred mypred
```

The plot is the 15 ticker predictions in the dataset, where the predictions are multivariate to multivariate. I have the model is scaled down, both in parameters in context length, so there is only 5 datapoints for each prediction.

## TODO, in order - Noon 3/16

-- need to read and write lines, it will be very inefficient to split('\n')

1. ~~scout - create ETTh1 dataset entry~~
2. ~~train - train on this dataset entry~~
3. ~~pred - pred on model, save predictions~~
4. ~~scout - save ETTH1 as pandasframe~~
5. ~~pred - quick visual, determine if scaler state is needed~~
6. ~~scout - get tickers.zip~~
7. ~~make sure steps 2-4 work for tickers~~
8. run training manually on lambda, verify loss, visualize it
9. make the trader good, parameterized visualization, backtesting
10. In general exhaust everything that can be verified with the existing dataset and infrastructure
11. figure out when get this far

## get and restructure the original data
If you want to download and restructure the old data... it is slow.
More notes in the file. It needs mongo. This code is largely outdated,
as with most things in /extras, but it should get the data from google
and run the agg pipelines.
```sh
pip install pymongo
```
https://www.mongodb.com/docs/manual/installation/
##### macos

```sh
brew tap mongodb/brew
brew install mongodb-community
```

run
```sh
python extras/get_new_data.py
```
