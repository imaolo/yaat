import gdown, os, subprocess, select, pprint, atexit, functools, pymongo

# helpers
def path(*fp): return '/'.join(fp)
def myprint(obj, header, pp=True):
    print(f"{'='*15} {header} {'='*15}")
    if pp: pprint.pprint(obj)
    else: print(obj)
def runcmd(cmd:str):
    def myass(proc): assert proc.returncode is None or proc.returncode == 0, f"Command failed - {cmd} \n\n returncode: {proc.returncode} \n\n stdout: \n {proc.stdout} \n\n stderr: \n{proc.stderr} \n\n"
    print(f"running command: {cmd}")
    proc = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    while True: # stream print
        myass(proc)
        if ready:=select.select([proc.stdout, proc.stderr], [], [], 0.1)[0]:
            if out:=ready[-1].readline(): print(out.strip()) # eh, only last one. if we miss some, whatever
        if proc.poll() is not None: break
    myass(proc)
    print(f"command succeeded: {cmd}")
def killproc(proc, proc_name):
    print(f"shutting down process: {proc_name}")
    proc.terminate()
    try: proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        print(f"process {proc_name} didn't terminate gracefully, killing it...")
        proc.kill()
    print(f"process {proc_name} shutdown")

# configs
db_name = 'yaatdb1'
data_name = 'tickers'
new_data_name = 'eth_tickers' # just eth for now
data_dir = 'data'
db_dir = path(data_dir, 'db')
num_data = 483380

# make data directory if needed
if not os.path.exists(data_dir): os.makedirs(data_dir)

# retrive & unzip
if not os.path.isfile(zip_fp:=path(data_dir, f"{data_name}.zip")):
    if not os.path.isfile(zip_fp):
        print("retrieving from gdrive.")
        gdown.download('https://drive.google.com/uc?id=1lgvxFp7l67dyZEGmvkqeP4fMN5hAQQ-q', zip_fp, quiet=False)
    runcmd(f"unzip {zip_fp} -d {data_dir}")

# make database directory
if not os.path.exists(db_dir): os.makedirs(db_dir)
    
# start database
print("starting mongo")
mongod = subprocess.Popen(['mongod', '--dbpath', db_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
assert mongod.poll() is None
print("mongo started")

# set exit hook to shutdown database
atexit.register(functools.partial(killproc, mongod, 'mongod'))

# connect to database
mongoc = pymongo.MongoClient()

# import into database
if db_name not in mongoc.list_database_names() or data_name not in mongoc[db_name].list_collection_names():
    runcmd(f"mongoimport --db {db_name} --collection {data_name} --drop --file {path(data_dir, f'{data_name}.json')}")

db = mongoc[db_name]
tickers = db[data_name]

# sanity check
print("counting docuemnts.")
assert tickers.count_documents({}) == num_data
print("documents counted.")

# clean & transform data - TODO - get more than just eth
if new_data_name not in db.list_collection_names():
    # extract just eth (TODO - get more than just eth - a doc.data unwind should do it.)
    tickers.aggregate([
        {'$set': {'data.ETH.bybit.epoch': {'$toLong': '$datetime'}}},
        {'$project': {'_id': 0, 'newDoc': '$data.ETH.bybit'}},
        {'$unset': ['newDoc.info', 'newDoc.timestamp']},
        {'$replaceRoot': {'newRoot': '$newDoc'}},
        {'$out': new_data_name}
    ])
    new_data = db[new_data_name]
    myprint(new_data.find_one(), new_data_name)

    # get the set of all keys in the collection
    keys = set(list(new_data.aggregate([
        {'$project': {'kvarr': {'$objectToArray': '$$ROOT'}}},
        {'$unwind': '$kvarr'},
        {'$group': {
            '_id': None,
            'collkeys': {'$addToSet': '$kvarr.k'}
        }},
    ]))[0]['collkeys'])
    keys.remove('_id')
    myprint(keys, f"{new_data_name} keys", False)

    # find keys that are null or non existent in any document
    bad_keys = {}
    for k in keys:
        res = list(new_data.aggregate([
            {'$match': {'$or': [{k: {'$exists': False}}, {k: None}]}},
            {'$count': 'num_docs'}
        ]))
        if res: bad_keys[k] = res[0]['num_docs']
    myprint(bad_keys, f"{new_data_name} null or dne keys", False)

    # unset bad keys
    unset_operation = {key: "" for key in bad_keys.keys()}
    new_data.update_many({}, {'$unset': {key: "" for key in bad_keys.keys()}})
    myprint(new_data.find_one(), f"{new_data_name} w/o bad keys")

# sanity check
print("counting docuemnts.")
assert db[new_data_name].count_documents({}) == num_data
print("documents counted.")

# export
if not os.path.isfile(fp:=path(data_dir, f"{new_data_name}.json")):
    runcmd(f"mongoexport --db {db_name} --collection {new_data_name} --out {fp}")