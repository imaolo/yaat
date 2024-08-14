# yaat - Yet Another Automated Trader

yaat is ana 
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

See yaat/main.py for detailed usage

yaat has three commands: train, predict, and plot_prediction.