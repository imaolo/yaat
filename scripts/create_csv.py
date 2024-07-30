
from yaat.maester import Maester
from tqdm import tqdm

## BE CAREFUL this drops production data!!!

maester = Maester(dbdir='spy_db')

tickers = ['SPY']
freq = '1min'
start_date = None

print(maester.candles_db.list_collection_names())
with tqdm(total=len(tickers)) as pbar:
    for tick in tickers:
        collname = f"{tick}_{freq}"
        pbar.set_postfix(status=tick)
        if collname in maester.candles_db.list_collection_names(): maester.candles_db[collname].drop()
        maester.create_tickers_dataset(tick, freq, start_date=start_date)
        pbar.update(1)
print("num inserted")
for tick in tickers:
    collname = f"{tick}_{freq}"
    print(collname, maester.candles_db[collname].count_documents({}))
print(maester.candles_db.list_collection_names())

df = maester.get_dataset(['SPY'])
df.to_csv('SPY1min.csv')