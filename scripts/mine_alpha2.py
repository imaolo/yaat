from yaat.maester import Maester
from datetime import datetime
from tqdm import tqdm

## BE CAREFUL this drops production data!!!

maester = Maester(connstr='mongodb://Earl:pink-Flamingo1317@52.91.137.11/')

tickers = ['SPY', 'TSLA']

print(maester.db.list_collection_names())
with tqdm(total=len(tickers)) as pbar:
    for tick in tickers:
        pbar.set_postfix(status=tick)
        if tick in maester.db.list_collection_names(): maester.db[tick].drop()
        maester.create_tickers_dataset(tick)
        pbar.update(1)
print("num inserted")
for tick in tickers:
    print(tick, maester.db[tick].count_documents({}))
print(maester.db.list_collection_names())
