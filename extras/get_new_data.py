import os, subprocess, atexit, functools, gdown
from util import path, myprint, runcmd, killproc

# not a formal dependency
try: import pymongo, pymongo.errors as mongoerrs
except: raise ImportError(f"{os.path.basename(__file__)} requires pymongo")

# The original tickers collection is poorly structured. We want to clean and restructure it into a new collection.
# We also don't want to start mongo all the time. This script starts the database, imports the data, processes it
# (agg pipelines) into a new collection, exports to a csv file, and zips it. The resultant zip file should be manually
# uploaded and be the main dataset.
    
db_name = 'yaatdb'
data_name = 'tickers_'
new_data_name = 'tickers'
data_dir = 'data'
model_dir = path(data_dir, 'model_data')
db_dir = path(data_dir, 'db')
data_fp = path(data_dir, data_name)
new_data_fp = path(data_dir, new_data_name)

def conn_db(dir):
    def getnping():
        (c := pymongo.MongoClient(serverSelectionTimeoutMS=5000)).admin.command('ping')
        return c
    try: return getnping()
    except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure):
        print(f"starting mongo @ {dir}")
        mongod = subprocess.Popen(['mongod', '--dbpath', dir], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        assert mongod.poll() is None
        print("mongo started")
        atexit.register(functools.partial(killproc, mongod, 'mongod'))
        return getnping()

if __name__ == '__main__':

    runcmd(f"rm -rf {data_dir}")
    os.makedirs(data_dir)
    os.makedirs(db_dir)
    os.makedirs(model_dir)

    gdown.download('https://drive.google.com/uc?id=1lgvxFp7l67dyZEGmvkqeP4fMN5hAQQ-q', data_fp+'.zip', quiet=False)

    runcmd(f"unzip {data_fp}.zip -d {data_dir}")
    runcmd(f"mv data/tickers.json {data_fp}.json") # its name is tickers.json

    mongoc = conn_db(db_dir)

    runcmd(f"mongoimport " \
        f"--db {db_name} " \
        f"--collection {data_name} " \
        f"--file {data_fp}.json " \
        f"--numInsertionWorkers {os.cpu_count()} " \
        "--drop --writeConcern 1")
    db = mongoc[db_name]
    data = db[data_name]

    # shorthands
    fields =  ['ask', 'baseVolume', 'bid', 'last', 'date', 'quoteVolume', 'symbol']
    conds = lambda f: [{f: {'$exists': True}}, {f: {'$ne': None}}]

    print("1. add date, promote data.*.bybit to root, and drop data.*.bybit.info and data.*.phemex")
    data.aggregate([
        {'$project': {
            'data': {'$objectToArray': '$data'},
            'date': {'$dateToString': {
                'format': '%Y-%m-%d %H:%M:%S',
                'date': '$datetime',
                'timezone': 'UTC'
            }}
        }},
        {'$unwind': '$data'},
        {'$unset': ['data.v.bybit.info', 'data.v.phemex']},
        {'$project': {'data': '$data.v.bybit', 'date': 1, '_id': 0}},
        {'$replaceRoot': {'newRoot': {'$mergeObjects': ['$$ROOT', '$data']}}},
        {'$project': {f:1 for f in fields}},
        {'$match': {'$and': [cond for f in fields for cond in conds(f)]}},
        {'$out': new_data_name}
    ])
    new_data = db[new_data_name]
    myprint(new_data_name, new_data.find_one())

    print("2. Get the set of unique symbols")
    symbols = db[new_data_name].distinct("symbol")
    exch_currs = [sym.split("/")[0] for sym in symbols]

    print("3. make new collections and export")
    postfix = lambda x: x+'_tickers'
    for sym, exch_sym in zip(exch_currs, symbols):
        db[new_data_name].aggregate([
            {'$match': {'symbol': exch_sym}},
            {'$project': {'symbol':0}},
            {'$out': postfix(sym)}
        ])
        doc = db[postfix(sym)].find_one()
        myprint(sym, doc)
        fields = doc.keys()
        runcmd(f"mongoexport " \
            f"--db {db_name} " \
            f"--collection {postfix(sym)} " \
            f"--type csv --fields {','.join(set(fields)-{'_id'})} " \
            f"--out {path(model_dir, postfix(sym))}.csv")

    runcmd(f"zip -r {model_dir}.zip {model_dir}")