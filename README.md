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

main.py is under construction, check later or fix it. yaat/ and test/ are working.

The maester provides information on available datasets, models, and predictions
We can can print all, some, or one of them.
It also downloads seed data if there is not existing data
```sh
python main.py maester
python main.py maester --models
python main.py maester --datasets
python main.py maester --preds
python main.py maester --preds --datasets
```

The seed datasets should be listed under datasets.


Use the cryp2020 dataset to train a model.

You will want to go in the data directory and cut down the size of the csv, otherwise training will take forever. It is not hard to navigate and is the same information presented by the maester. This will create extraneous .npy files in pwd. They can be ignored and deleted.

```sh
python main.py train --name mymodelname --dataset cryp2020
python main.py maester --models
```

Use the model to make predictions.

```sh
python main.py maester --models --datasets
python main.py pred --name mypred --dataset cryp2020 --model mymodelname
python main.py maester --preds
```

Visualize the predictions
```sh
python main.py trade --pred mypred
```

The plot is the 15 ticker predictions in the dataset, where the predictions are multivariate to multivariate. I have the model is scaled down, both in parameters in context length, so there is only 5 datapoints for each prediction.

## docker

```sh
docker build -t yaat-image  .
docker run -d --name yaat-node yaat-image python3 main.py train --name lambdock1 --dataset cryp2020
docker logs yaat-node
```

## TODO

 1. ~~scout - create ETTh1 dataset entry~~
 2. ~~train - train on this dataset entry~~
 3. ~~pred - pred on model, save predictions~~
 4. ~~scout - save ETTH1 as pandasframe~~
 5. ~~pred - quick visual, determine if scaler state is needed~~
 6. ~~scout - get tickers.zip~~
 7. ~~make sure steps 2-4 work for tickers~~
 8. ~~run training manually on lambda, verify loss, visualize it~~
        - test loss down to ~0.6 at best, we want to do better, need better data, audible
 9. miner - grab as much historical data as possible for ^GSPC, ^IXIC, ^DJI, BTC-USD, ETH-USD, DX-Y.NYB,
    EURUSD=X, USDCNY=X, GC=F, CL=F, and ^SPNY. Get price, volume, esp, gas fees, and public sentiment.
10. clean data into maester
12. repeat 2-5, verify loss
13. trader - visualize, back test, sandbox, deploy

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
