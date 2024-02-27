import os
from util import path, myprint, runcmd, conn_db
try: import gdown
except: raise ImportError(f"{os.path.basename(__file__)} requires gdown")

# The original tickers collection is poorly structured. We want to clean and restructure it into a new collection.
# We also don't want to start mongo all the time. This script starts the database, imports the data, processes it (agg pipelines)
# into a new collection, exports to a csv file, and zips it. The resultant zip file should be manually uploaded and be the main dataset.
    
db_name = 'yaatdb'
data_name = 'old_tickers'
new_data_name = 'tickers'
data_dir = 'data'
db_dir = path(data_dir, 'db')
data_fp = path(data_dir, data_name)
new_data_fp = path(data_dir, new_data_name)

if __name__ == '__main__':

    runcmd(f"rm -rf {data_dir}")
    os.makedirs(data_dir)
    os.makedirs(db_dir)

    gdown.download('https://drive.google.com/uc?id=1lgvxFp7l67dyZEGmvkqeP4fMN5hAQQ-q', data_fp+'.zip', quiet=False)

    runcmd(f"unzip {data_fp}.zip -d {data_dir}")
    runcmd(f"mv {new_data_fp}.json {data_fp}.json") # it has the name we want to use

    mongoc = conn_db(db_dir)

    runcmd(f"mongoimport --db {db_name} --collection {data_name} --drop --file {data_fp}.json --numInsertionWorkers {os.cpu_count()} --writeConcern 1")
    db = mongoc[db_name]
    data = db[data_name]

    print("1. add epoch, promote data.*.bybit to root, and drop data.*.bybit.info and data.*.phemex")
    data.aggregate([
        {'$project': {'data': {'$objectToArray': '$data'}, 'epoch': {'$toLong': '$datetime'}}},
        {'$unwind': '$data'},
        {'$unset': ['data.v.bybit.info', 'data.v.phemex']},
        {'$project': {'data': '$data.v.bybit', 'epoch': 1, '_id': 0}},
        {'$replaceRoot': {'newRoot': {'$mergeObjects': ['$$ROOT', '$data']}}},
        {'$project': {'data': 0}},
        {'$out': new_data_name}
    ])
    new_data = db[new_data_name]
    myprint(new_data_name, new_data.find_one())

    print("2. get the set of document keys")
    keys = set(list(new_data.aggregate([
        {'$project': {'kvarr': {'$objectToArray': '$$ROOT'}}},
        {'$unwind': '$kvarr'},
        {'$group': {'_id': None, 'collkeys': {'$addToSet': '$kvarr.k'}}},
    ]))[0]['collkeys']) - {'_id'}
    myprint(f"{new_data_name} keys", keys)

    print("3. get the set of keys which have null or dne document values")
    key_ndne_counts: dict = list(new_data.aggregate([{'$facet': {k: [
        {'$match': {'$or': [{k: {'$exists': False}}, {k: None}]}},
        {'$count': 'count'}
    ] for k in keys}}]))[0]
    bad_keys = {k:v[0]['count'] for k, v in key_ndne_counts.items() if v}
    myprint(f"{new_data_name} null or dne keys and counts", bad_keys)

    print("4. unset null or dne document attributes")
    new_data.update_many({}, {'$unset': {key: "" for key in bad_keys.keys()}})
    myprint(f"{new_data_name} w/o bad keys", new_data.find_one())
    
    print("5. verifying transformation")
    assert db[new_data_name].count_documents({}) == 8123464

    fields = mongoc[db_name][new_data_name].find_one().keys()
    runcmd(f"mongoexport --db {db_name} --collection {new_data_name} --type csv --fields '{','.join(set(fields)-{'_id'})}' --out {new_data_fp}.csv")

    runcmd(f"zip {new_data_fp}.zip {new_data_fp}.csv")