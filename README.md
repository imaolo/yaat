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
```sh
python main maester
python main maester --models
python main maester --datasets
python main maester --preds
python main maester --preds --datasets
```

They should return nothing as we've created nothing.

The scout creates the ETTh1 dataset

```sh
python main scout
python main maester --datasets
```

Use the ETTh1 dataset to train a model
This will create extraneous .npy files in pwd.
They can be ignored and deleted.

```sh
python main train --name mymodelname --dataset ETTh1
python main maester --models
```

Use the model to make predictions.

```sh
python main maester --models --datasets
python main pred --name mypred --dataset ETTh1 --model mymodelname
python main maester --preds
```

TODO visualize

## TODO, in order - Noon 3/16

-- need to read and write lines, it will be very inefficient to split('\n')

1. ~~scout - create ETTh1 dataset entry~~
2. ~~train - train on this dataset entry~~
3. ~~pred - pred on model, save predictions~~
3. pred - quick visual, determine if scaler state is needed
4. scout - get tickers.zip
5. make sure steps 2-4 work for tickers
6. setup rclone (dbx and integrate, build, & push docker)
7. make sure steps 1-5 work locally, with docker
8. run training manually on lambda
10. monitor from the client
11. run prediction manually on lambda
12. visualize & pray the results are good
13. Bazaar!

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
