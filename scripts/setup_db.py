# nohup python3.11 scripts/setup_db.py > setup_db.log 2>&1 &

from bson.int64 import Int64
from datetime import datetime, time
from botocore.config import Config
import pymongo, boto3, tqdm, tempfile, pandas as pd

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

# get s3 client
s3 = boto3.Session(
   aws_access_key_id='decdeaa4-82e8-4fe8-b2f6-386f9e6db6a0',
   aws_secret_access_key='fuqZHZzJdzJpYq2kMRxZTI42N1nPlxKj',
).client(
   's3',
   endpoint_url='https://files.polygon.io',
   config=Config(signature_version='s3v4'),
)

# only 5 years back allowed
start_date = datetime.combine((now:=datetime.now()).replace(year=now.year-5).date(), time())

# helper to filter files
def is_valid_fn(fn:str) -> bool:
   return 'minute_aggs_v1' in fn and datetime.strptime(fn.split('/')[-1].split('.')[0], "%Y-%m-%d") > start_date

# loop through minute aggregate files
paginator = s3.get_paginator('list_objects_v2')
prefix = 'us_stocks_sip'
temp_file_path = tempfile.NamedTemporaryFile(delete=False).name + '.csv.gz'
filenames = [obj['Key'] for page in paginator.paginate(Bucket='flatfiles', Prefix=prefix) for obj in page['Contents'] if is_valid_fn(obj['Key'])]
for fn in tqdm.tqdm(filenames, desc="Downloading files"): 
    # dowload the file
    s3.download_file('flatfiles', fn, temp_file_path)

    # process into list of dictionaries
    df = pd.read_csv(temp_file_path)
    df[floatcols] = df[floatcols:=['open', 'close', 'high', 'low']].astype(float)
    df['window_start'] = pd.to_datetime(df['window_start'], unit='ns')
    records = df.to_dict('records')  # long fields must be casted after to_dict - adds 7-10 seconds to each loop :(
    for record in records:
        record['volume'] = Int64(record['volume'])
        record['transactions'] = Int64(record['transactions'])

    # insert
    coll.insert_many(records)

# create indexes after writes
coll.create_index({'ticker':1, 'window_start':1}, unique=True)
coll.create_index({'ticker':1})
coll.create_index({'window_start':1})