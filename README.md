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

Train TODO
```sh
python main.py train # model definition, model nam , other metadata, local or remote, dataset, etc
```

Pred TODO
```sh
python main.py pred # models name, dataset
```

Scout TODO
```sh
python main.py scout # dataset name, dataset definition, metadata, etc
```

Scout TODO
```sh
python main.py trade # data, strategy definition, model, etc
```

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
