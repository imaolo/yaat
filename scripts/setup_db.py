# pip install pymongo
# pip install tqdm
# pip install pandas

from pathlib import Path
import pymongo, tqdm, pandas as pd
from bson.int64 import Int64

# configs
FROM_DIR = Path('~/Projects/polygon_tickers_1min').expanduser()
assert FROM_DIR.exists(), FROM_DIR

# connect to the database
dbc = pymongo.MongoClient('localhost:27017')

# get the yaat db
db = dbc['yaatdb']

# define the candles schema
schema = {
    'title': 'Candles every 1 minute',
    'required': ['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions'],
    'properties': {
        'ticker':       {'bsonType': 'string'},
        'volume':       {'bsonType': 'long'},
        'open':         {'bsonType': 'double'},
        'close':        {'bsonType': 'double'},
        'high':         {'bsonType': 'double'},
        'low':          {'bsonType': 'double'},
        'window_start': {'bsonType': 'date'},
        'transactions': {'bsonType': 'long'},
    }
}

# create the candles collection
collname = 'candles1min'
assert collname not in (names:=db.list_collection_names()), names
coll = db.create_collection(collname, validator={'$jsonSchema': schema})
coll.create_index({'ticker':1, 'window_start':1}, unique=True)
coll.create_index({'ticker':1})
coll.create_index({'window_start':1})

# read each file into the database
filenames = [fn for fn in FROM_DIR.rglob('*.csv.*')]
for fn in tqdm.tqdm(filenames, desc="combining dataframes"): 
    df = pd.read_csv(fn)

    # type casting
    df[floatcols] = df[floatcols:=['open', 'close', 'high', 'low']].astype(float)
    df['window_start'] = pd.to_datetime(df['window_start'], unit='ns')
    
    # long fields must be casted after to_dict - adds 7-10 seconds to each loop :(
    records = df.to_dict('records')
    for record in records:
        record['volume'] = Int64(record['volume'])
        record['transactions'] = Int64(record['transactions'])

    coll.insert_many(records)