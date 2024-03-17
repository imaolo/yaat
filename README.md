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

The maester provides information on available datasets and models
```sh
python main.py maester # list datasets
python main.py maester --models # list models
python main.py maester --datasets --models # list both
```

Unfortunately main.py doesnt support creating (training) models or
scouting for datasets yet, so the lists should be empty.

In the meantime, if you want to lists models and/or datasets:

Run the tests with DEBUG=1 and the test directory will not be cleaned when the tests complete. It should be apparent what the test directory is in pwd.

See test_maester.py to find which directories are the maester's. See maester.py to point the maester to the correct directory. The commands should then work.

You can also create models and datasets manually. The informer is implemented (just not in main.py, yet), and see test_maester.py for how to created sample datasets.

## TODO, in order - Noon 3/16

-- need to read and write lines, it will be very inefficient to split('\n')

1. scout - create ETTh1 dataset entry
2. train - train on this dataset entry
3. pred - pred on model entry and dataset entry
4. scout - get tickers.zip
5. make sure steps 2-4 work for tickers
6. setup rclone (dbx and integrate, build, & push docker)
7. make sure steps 1-5 work locally, with docker
8. run training manually on lambda
10. monitor from the client
11. run prediction manually on lambda
12. visualize & pray the results are good
13. Bazaar!

### infrastructure

- csv needs to be better handled by the dataset entry
- dataset entry needs args attribute (for mean and std)

#### Scout - no dependencies
create data entries
```sh
python main.py scout # what data to get
```

#### Train - Me - scout dependency
- need existing data entries to configure the scalar state etc..s
```sh
python main.py train # model definition, model nam , other metadata, local or remote, dataset, etc
```

#### Pred - Me - train and scount dependency
need to implement main.py and restore the scalers state. preliminary de scaling tests show the outputs are in the correct range (ball parked). More importantly, the scaled loss is very low. The main task here is to get the scalers state from the model and write the descaled predictions to a file.
There may be some stuff where you need to wait for the context to be filled, and that could require extra logic.
```sh
python main.py pred # models name, dataset
```

#### Trade - dependent on all before
```sh
python main.py trade # model, strategy definition, auth, visualize, etc 
```

#### DevOps/Maester/Bazaar - dependent on all before
rclone, docker, cloud

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
