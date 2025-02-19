import requests, json, pymongo
from dataclasses import asdict
from datetime import datetime
from pprint import pprint

# # URL of your Flask API
# url = 'http://localhost:8080/s3'  # Change the port if needed

# # Making a POST request with JSON data
# response_post = requests.get(url, json=json.dumps(['one', 'two']))
# print("Response from POST request:", response_post.text)

# mongoexport --uri="mongodb://Earl:pink-Flamingo1317@54.208.14.52:27017/yaatdb?authSource=admin" --collection=candles1min --query='{ "ticker": "SPY" }' --type=csv --fields="volume,open,close,high,low,window_start,transactions" --sort='{"window_start": 1}' --out=spy.csv

candles = pymongo.MongoClient('mongodb://Earl:pink-Flamingo1317@54.208.14.52:27017')['yaatdb']['candles1min']

candles.aggregate([
    {'$match': {'ticker': 'SPY'}}
])

docs = list(candles.aggregate([
    {'$match'}
    {'$group': {
        '_id': '$ticker',
        'count': {'$sum': 1}
    }},
    {'$setWindowFields': {
        'partitionBy': None,  # No partition, operate over the entire collection
        'sortBy': {'value': 1},
        'output': {
            'differenceWithPrevious': {
                '$subtract': [
                    '$count',
                    {'$shift': {'output': '$count', 'by': -1, 'default': None}}
                ]
            }
        }
    }}
]))

pprint(docs)

