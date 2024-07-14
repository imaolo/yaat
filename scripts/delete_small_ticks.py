from yaat.maester import Maester
from pprint import pprint

maester = Maester('mongodb://Earl:pink-Flamingo1317@52.91.137.11/')

ticker_counts = list(maester.candles1min.aggregate([
            {'$group': {
                '_id': '$ticker',
                'count': {'$sum': 1},
            }},
            {'$project': {
                'ticker': '$_id',
                'count': 1,
                '_id': 0
            }},
            {'$match': {'count': {'$lt': 100000}}}
        ]))

tot = 0
for tick_count in ticker_counts:
    print(f"deleting {tick_count['ticker']}")
    tot += maester.candles1min.delete_many({'ticker': tick_count['ticker']}).deleted_count
print("total deleted: ", tot)