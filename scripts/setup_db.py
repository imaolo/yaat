# nohup python3.11 scripts/setup_db.py > setup_db.log 2>&1 &

from bson.int64 import Int64
from datetime import datetime, time
from botocore.config import Config
import boto3, tqdm, tempfile, pandas as pd
from yaat.maester import Maester

# connect to the database
maester = Maester(connstr='mongodb://Earl:pink-Flamingo1317@localhost:27017/')

# clean the candles collection
maester.candles1min.delete_many({})
maester.candles1min.drop_indexes()

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

    # create the dataframe
   df = pd.read_csv(temp_file_path)

   # rename window start to date
   df.rename(columns={'window_start': 'date'}, inplace=True)

   # set the types
   df[floatcols] = df[floatcols:=['open', 'close', 'high', 'low']].astype(float)
   df['date'] = pd.to_datetime(df['date'], unit='ns')

   # make into list of dicts
   records = df.to_dict('records')  # long fields must be casted after to_dict - adds 7-10 seconds to each loop :(
   for record in records:
       record['volume'] = Int64(record['volume'])
       record['transactions'] = Int64(record['transactions'])

   # insert
   batch_size = 1000
   for i in range(0, len(records), batch_size):
      batch_records = records[i:i+batch_size]
      try: maester.candles1min.insert_many(batch_records, ordered=False)
      except Exception as e: print(f"failed processing: {fn} - {e}")
print("insertion complete")

# add the indexes back after the writes are completed   

maester.candles1min.create_index(idx:={'ticker':1, 'date':1}, unique=True)
print(f"created index: {idx}")

maester.candles1min.create_index(idx:={'ticker':1})
print(f"created index: {idx}")

maester.candles1min.create_index(idx:={'date':1})
print(f"created index: {idx}")