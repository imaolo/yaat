import pymongo, pprint, subprocess

# helper
def myprint(obj, header, pp=True):
    print(f"{'='*15} {header} {'='*15}")
    if pp: pprint.pprint(obj)
    else: print(obj)

# connect to database
db = pymongo.MongoClient()['yaatdb1']
tickers = db['tickers']

# extract eth on bybit (arbitrary selection)
if 'eth_tickers' not in db.list_collection_names():
    tickers.aggregate([
        {'$set': {'data.ETH.bybit.epoch': {'$toLong': '$datetime'}}},
        {'$project': {'_id': 0, 'newDoc': '$data.ETH.bybit'}},
        {'$unset': ['newDoc.info', 'newDoc.timestamp']},
        {'$replaceRoot': {'newRoot': '$newDoc'}},
        {'$out': 'eth_tickers'}
    ])
eth_tickers = db['eth_tickers']
myprint(eth_tickers.find_one(), "eth_tickers")

# get the set of all keys in the collection
keys = set(list((eth_tickers.aggregate([
    {'$project': {'kvarr': {'$objectToArray': '$$ROOT'}}},
    {'$unwind': '$kvarr'},
    {'$group': {
        '_id': None,
        'collkeys': {'$addToSet': '$kvarr.k'}
    }},
])))[0]['collkeys'])
keys.remove('_id')
myprint(keys, "eth_tickers keys", False)

# find which keys are null or non existent
bad_keys = {}
for k in keys:
    res = list(eth_tickers.aggregate([
        {'$match': {'$or': [{k: {'$exists': False}}, {k: None}]}},
        {'$count': 'num_docs'}
    ]))
    if res: bad_keys[k] = res[0]['num_docs']
myprint(bad_keys, "eth_tickers null or dne keys", False)

# unset bad keys
unset_operation = {key: "" for key in bad_keys.keys()}
eth_tickers.update_many({}, {'$unset': {key: "" for key in bad_keys.keys()}})
myprint(eth_tickers.find_one(), "eth_tickers w/o bad keys")

# export eth_tickers
cmd = 'mongoexport --db=yaatdb1 --collection=eth_tickers --out=data/eth_tickers.json'
res = subprocess.run(cmd.split(' '))
assert not res.returncode, f"Command failed - {cmd} \n\n stdout: \n {res.stdout} \n\n stderr: \n{res.stderr} \n\n"