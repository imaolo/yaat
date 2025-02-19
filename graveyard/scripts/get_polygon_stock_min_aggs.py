# pip install boto3
# pip install tqdm

# Fetch your S3 Access and Secret keys from
# https://polygon.io/dashboard/flat-files

# Quickstart
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html

from datetime import datetime, time
from pathlib import Path
from botocore.config import Config
import boto3, tqdm

# zip files downloaded here
DIR = Path('~/Projects/polygon_tickers_1min').expanduser()
DIR.mkdir(parents=True, exist_ok=False)

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

# loop through minute aggregates and download the desired files
paginator = s3.get_paginator('list_objects_v2')
prefix = 'us_stocks_sip'
filenames = [obj['Key'] for page in paginator.paginate(Bucket='flatfiles', Prefix=prefix) for obj in page['Contents'] if is_valid_fn(obj['Key'])]
for fn in tqdm.tqdm(filenames, desc="Downloading files"):      
   local_path = DIR / fn
   local_path.parent.mkdir(parents=True, exist_ok=True)
   s3.download_file('flatfiles', fn, str(local_path.absolute()))