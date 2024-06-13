from flask import Flask, request
from dataclasses import dataclass
from yaat.util import clean_date, DATE_FORMAT
from datetime import datetime
from pprint import pprint
import pymongo, json

app = Flask(__name__)
db = pymongo.MongoClient('localhost:27017')['yaatdb']
candles = db['candles1min']

@dataclass
class DB_Req:
    pw: str
    tickers: list[str]
    start_date: str
    end_date: str

    def __post_init__(self):
        self.start_date = clean_date(self.start_date)
        self.end_date = clean_date(self.end_date)

@app.route('/', methods=['POST'])
def listener():
    data = request.get_json()
    dbreq = DB_Req(**json.loads(data))

    # construct and dispatch mongo query

    # create the collection
    collname = '_'.join(dbreq.tickers + [dbreq.start_date, dbreq.end_date])
    assert collname not in (names:=db.list_collection_names()), f"{collname} already in database - {names}"

    # make sure the tickers are present
    for tick in dbreq.tickers: assert candles.find_one({'tickers': tick}), f"{tick} not in candles"

    # create the output collection
    candles.aggregate([
        {'$match': {
            'tickers': {'$in': dbreq.tickers},
            'window_start':{
                '$gte': datetime.strptime(dbreq.start_date, DATE_FORMAT),
                '$lte': datetime.strptime(dbreq.end_date, DATE_FORMAT)
            }
        }},
        {'$out': collname}
    ])

    # make sure all the groups have the same amount of tickers, probably not
    ticker_groups = list(db[collname].aggregate([
        {'$group': {
            '_id': '$tickers',
            'count': {'$sum': 1}
        }}
    ]))
    count = ticker_groups[0]['count']
    for tg in ticker_groups:
        if count != tg['count']:
            pprint(ticker_groups)
            raise RuntimeError("tickers have different number of entries")

    # export and zip result

    

    # upload to s3

    # return s3 path

    print(f"Data Received: {dbreq}")
    return "s3 path"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)