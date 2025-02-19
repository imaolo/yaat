from flask import Flask, request, abort, jsonify
import pymongo, json

app = Flask(__name__)
candles = pymongo.MongoClient('mongodb://Earl:pink-Flamingo1317@54.208.14.52:27017')['yaatdb']['candles1min']


@app.route('/s3', methods=['GET'])
def s3_listener():
    query = json.loads(request.get_json())
    try: candles.aggregate(query + [{'$out': 'tmpcoll'}])
    except Exception as e:
        response = jsonify({"error": str(e)})
        response.status_code = 500
        return response

    # zip, upload to s3, return url

    # zip the data
    return 'bleh'

@app.route('/db', methods=['GET'])
def db_listener():
    query = json.loads(request.get_json())
    try: res = list(candles.aggregate(query))
    except Exception as e:
        response = jsonify({"error": str(e)})
        response.status_code = 500
        return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)




# ptest
    # # construct and dispatch mongo query

    # # create the collection
    # collname = '_'.join(dbreq.tickers + [dbreq.start_date, dbreq.end_date])
    # assert collname not in (names:=db.list_collection_names()), f"{collname} already in database - {names}"

    # # make sure the tickers are present
    # for tick in dbreq.tickers: assert candles.find_one({'tickers': tick}), f"{tick} not in candles"

    # # create the output collection
    # candles.aggregate([
    #     {'$match': {
    #         'tickers': {'$in': dbreq.tickers},
    #         'window_start':{
    #             '$gte': datetime.strptime(dbreq.start_date, DATE_FORMAT),
    #             '$lte': datetime.strptime(dbreq.end_date, DATE_FORMAT)
    #         }
    #     }},
    #     {'$out': collname}
    # ])

    # # make sure all the groups have the same amount of tickers, probably not
    # ticker_groups = list(db[collname].aggregate([
    #     {'$group': {
    #         '_id': '$tickers',
    #         'count': {'$sum': 1}
    #     }}
    # ]))
    # count = ticker_groups[0]['count']
    # for tg in ticker_groups:
    #     if count != tg['count']:
    #         pprint(ticker_groups)
    #         raise RuntimeError("tickers have different number of entries")

    # export and zip result

    

    # upload to s3

    # return s3 path
