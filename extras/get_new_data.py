import os, subprocess, atexit, functools, pandas, torch, itertools
from yaat.util import path, myprint, runcmd, killproc

# not formal dependencies
try: import gdown, pymongo, pymongo.errors as mongoerrs
except: raise ImportError(f"{os.path.basename(__file__)} requires gdown and pymongo")

# The original tickers collection is poorly structured. We want to clean and restructure it into a new collection.
# We also don't want to start mongo all the time. This script starts the database, imports the data, processes it
# (agg pipelines) into a new collection, exports to a csv file, and zips it. The resultant zip file should be manually
# uploaded somewhere accessible

atlas_uri = "mongodb+srv://matt-yaat:hxCobeNJBzdVYQmR@yaatdb.iomebug.mongodb.net/?retryWrites=true&w=majority&appName=yaatdb"
db_name = 'yaatdb'
og_data_name = 'og_tickers'
cleaned_data_name = 'cleaned_tickers'
final_data_name = 'tickers'
data_dir = 'data'
db_dir = path(data_dir, 'db')
og_data_fp = path(data_dir, og_data_name)
cleaned_data_fp = path(data_dir, cleaned_data_name)
final_data_fp = path(data_dir, final_data_name)
ticker_fields =  ['last']
shared_fields = ['symbol', 'date']
fields = ticker_fields + shared_fields

def conn_db(dir, uri=None):
    def getnping():
        (c := pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)).admin.command('ping')
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

    print("0. setup (clean and make directories, download original data, start db, etc)")
    runcmd(f"rm -rf {data_dir}")
    os.makedirs(data_dir)
    os.makedirs(db_dir)
    gdown.download('https://drive.google.com/uc?id=1lgvxFp7l67dyZEGmvkqeP4fMN5hAQQ-q', og_data_fp+'.zip', quiet=False)
    runcmd(f"unzip {og_data_fp}.zip -d {data_dir}")
    runcmd(f"mv data/tickers.json {og_data_fp}.json") # its name is tickers.json
    db = conn_db(db_dir)[db_name]
    runcmd(f"mongoimport " \
        f"--db {db_name} " \
        f"--collection {og_data_name} " \
        f"--file {og_data_fp}.json " \
        f"--numInsertionWorkers {os.cpu_count()} " \
        f"--drop --writeConcern 1")

    print("1. clean original data (promote date and data.*.bybit, drop data.*.bybit.info and data.*.phemex, etc")
    conds = lambda f: [{f: {'$exists': True}}, {f: {'$ne': None}}]
    db[og_data_name].aggregate([
        {'$project': {'data': {'$objectToArray': '$data'}, 'date': '$datetime'}},
        {'$unwind': '$data'},
        {'$unset': ['data.v.bybit.info', 'data.v.phemex', 'data.v.bybit.date']},
        {'$project': {'data': '$data.v.bybit', 'date': 1, '_id': 0}},
        {'$replaceRoot': {'newRoot': {'$mergeObjects': ['$$ROOT', '$data']}}},
        {'$project': {f:1 for f in fields}},
        {'$match': {'$and': [cond for f in fields for cond in conds(f)]}},
        {'$out': cleaned_data_name}
    ])
    cleaned_data = db[cleaned_data_name]
    myprint(cleaned_data_name, cleaned_data.find_one())
    runcmd(f"mongoexport " \
        f"--db {db_name} " \
        f"--collection {cleaned_data_name} " \
        f"--type json " \
        f"--out {cleaned_data_fp}.json")

    print("2. get the set of unique symbols")
    if "symbol" not in cleaned_data.list_indexes(): cleaned_data.create_index("symbol")
    symbols = cleaned_data.distinct("symbol")
    currs = [s.split('/')[0] for s in symbols]
    print(currs)

    print("3. create a collection for each symbol")
    coll_names = []
    for sym, curr in zip(symbols, currs):
        coll_name = f"{curr}_{final_data_name}"
        new_fields = [f"{curr}_{field}" for field in ticker_fields]

        schema = {'$jsonSchema': {
            'bsonType': 'object',
            'title': f"{sym} crypto ticker exchange data",
            'required': new_fields + shared_fields,
            'properties': {field: {'bsonType': ['double'], 'description': "model data"} for field in new_fields}
        }}
        if coll_name in db.list_collection_names(): db.drop_collection(coll_name)
        db.create_collection(name=coll_name, validator=schema, validationAction='error')

        cleaned_data.aggregate([
            {'$match': {'symbol': sym}},
            {'$project': {nf:{'$toDouble':'$'+f} for f, nf in zip(ticker_fields, new_fields)} | {sf:1 for sf in shared_fields}},
            {'$out': coll_name}
        ])
        if (coll:=db[coll_name]).count_documents({}) != 483380: db.drop_collection(coll_name)
        else:
            coll_names.append(coll_name)
            coll.create_index({'date':1})
            myprint(coll_name, coll.find_one())

    print("4. create final data collection")
    myprint('collection names', coll_names)
    new_fields = [f"{cn.split('_')[0]}_{field}" for field in ticker_fields for cn in coll_names]
    schema = {'$jsonSchema': {
        'bsonType': 'object',
        'title': f"{sym} crypto ticker exchange data",
        'required': new_fields + shared_fields,
        'properties': {field: {'bsonType': ['double'], 'description': "model data"} for field in new_fields}
    }}
    if final_data_name in db.list_collection_names(): db.drop_collection(final_data_name)
    db.create_collection(name=final_data_name, validator=schema, validationAction='error')

    print("5. join the symbol collections into the final data collection, and export")
    def _construct_join_merge(cn):
        curr = cn.split('_')[0]
        new_fields = [f"{curr}_{field}" for field in ticker_fields]
        return (
            {'$lookup': {
                'from': cn,
                'localField': 'date',
                'foreignField': 'date',
                'as': cn
            }},
            {'$set': {cn: {'$arrayElemAt': ['$'+cn, 0]}}},
            {'$replaceRoot': {'newRoot': {'$mergeObjects': [
                '$$ROOT',
                {field: f"${cn}.{field}" for field in new_fields}
            ]}}},
        )
    db[coll_names[0]].aggregate([
        *list(itertools.chain.from_iterable(map(_construct_join_merge, coll_names[1:]))),
        {'$project': {
            'date': {'$dateToString': {
                'format': '%Y-%m-%d %H:%M:%S',
                'date': '$datetime',
                'timezone': 'UTC'
            }},
            **{nf:1 for nf in new_fields + shared_fields}
        }},
        {'$out': final_data_name}
    ])
    myprint(final_data_name, doc:=db[final_data_name].find_one())
    runcmd(f"mongoexport " \
        f"--db {db_name} " \
        f"--collection {final_data_name} " \
        f'--type csv --fields "{",".join(list(set(doc.keys())-set(["_id", "symbol"])))}" ' \
        f"--out {final_data_fp}.csv")