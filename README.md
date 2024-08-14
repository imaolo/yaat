# YAAT - Yet Another Automated Trader
## install
```sh
python -m pip install -e . 
python -m pip install -e '.[testing]' # for testing
```

## testing
A testing database will need to be configured for the tests to run.
A few of the tests will not pass because the api subscriptions
have been cancelled.
```sh
python -m pytest test/ 
```

## usage

See ./main.py and yaat/main.py for detailed usage.

YAAT has three commands: train, predict, and plot_prediction.