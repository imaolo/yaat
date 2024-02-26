import gdown, os, subprocess, select, pprint, atexit, functools, torch, pandas, typing
import pymongo, pymongo.collection as mongocoll, pymongo.database as mongodb, pymongo.errors as mongoerrs

# helpers

def _path(*fp): return '/'.join(fp)
def _myprint(header, obj):
    print(f"{'='*15} {header} {'='*15}")
    pprint.pprint(obj)
def _runcmd(cmd:str):
    def ass(proc): assert proc.returncode is None or proc.returncode == 0, f"Command failed - {cmd} \n\n returncode: {proc.returncode} \n\n stdout: \n {proc.stdout.read()} \n\n stderr: \n{proc.stderr.read()} \n\n"
    print(f"running command: {cmd}")
    proc = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    while True: # stream print
        ass(proc)
        if ready:=select.select([proc.stdout, proc.stderr], [], [], 0.1)[0]:
            if out:=ready[-1].readline(): print(out.strip()) # eh, only last one. if we miss some, whatever
        if proc.poll() is not None: break
    ass(proc)
    print(f"command succeeded: {cmd}")
def _killproc(proc, proc_name):
    print(f"shutting down process: {proc_name}")
    proc.terminate()
    try: proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        print(f"process {proc_name} didn't terminate gracefully, killing it...")
        proc.kill()
    print(f"process {proc_name} shutdown")
def _pt_load_save(fp_from, fp_to):
    print(f"reading {fp_from} into {fp_to}")
    data = torch.tensor(pandas.read_csv(fp_from)['last'].to_numpy(), dtype=torch.float32)
    torch.save(data, fp_to) 
    return data

# configs

db_name = 'yaatdb1'
data_name = 'tickers'
new_data_name = 'new_tickers'
data_dir = 'data'
db_dir = _path(data_dir, 'db')
zfp = f"{(data_fp:=_path(data_dir, data_name))}.zip"
jsfp = f"{data_fp}.json"
new_ptfp = f"{(new_data_fp:=_path(data_dir, new_data_name))}.pt"
new_csvfp = f"{new_data_fp}.csv"
num_docs = 8123464

# functions

def clean(name:str, in_db=False):
    if in_db: conn_db()[db_name][name].drop()
    else: _runcmd(f"rm -rf {name}")

def conn_db():
    def ping(client):
        client.admin.command('ping')
        return client
    try: return ping(pymongo.MongoClient(serverSelectionTimeoutMS=5000))
    except (mongoerrs.ServerSelectionTimeoutError, mongoerrs.ConnectionFailure):
        print("starting mongo")
        mongod = subprocess.Popen(['mongod', '--dbpath', db_dir], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        assert mongod.poll() is None
        print("mongo started")
        atexit.register(functools.partial(_killproc, mongod, 'mongod'))
        return ping(pymongo.MongoClient(serverSelectionTimeoutMS=5000))

def transform_data(db:mongodb.Database, data:mongocoll.Collection, new_data_name:str, num_docs=typing.Optional[int]):
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
    _myprint(new_data_name, new_data.find_one())

    print("2. get the set of document keys")
    keys = set(list(new_data.aggregate([
        {'$project': {'kvarr': {'$objectToArray': '$$ROOT'}}},
        {'$unwind': '$kvarr'},
        {'$group': {'_id': None, 'collkeys': {'$addToSet': '$kvarr.k'}}},
    ]))[0]['collkeys']) - {'_id'}
    _myprint(f"{new_data_name} keys", keys)

    print("3. get the set of keys which have null or dne document values")
    key_ndne_counts: dict = list(new_data.aggregate([{'$facet': {k: [
        {'$match': {'$or': [{k: {'$exists': False}}, {k: None}]}},
        {'$count': 'count'}
    ] for k in keys}}]))[0]
    bad_keys = {k:v[0]['count'] for k, v in key_ndne_counts.items() if v}
    _myprint(f"{new_data_name} null or dne keys and counts", bad_keys)

    print("4. unset null or dne document attributes")
    new_data.update_many({}, {'$unset': {key: "" for key in bad_keys.keys()}})
    _myprint(f"{new_data_name} w/o bad keys", new_data.find_one())

    if num_docs:
        print("5. verifying transformation")
        assert db[new_data_name].count_documents({}) == num_docs

def fetch() -> torch.Tensor:
    if not os.path.exists(data_dir): os.makedirs(data_dir)
    if not os.path.exists(db_dir): os.makedirs(db_dir)

    if os.path.isfile(new_ptfp):
        print(f"loading from {new_ptfp}")
        return torch.load(new_ptfp)
    if os.path.isfile(new_csvfp): return _pt_load_save(new_csvfp, new_ptfp)

    mongoc = conn_db()

    def export_load_save(fields):
        _runcmd(f"mongoexport --db {db_name} --collection {new_data_name} --type csv --fields '{','.join(set(fields)-{'_id'})}' --out {new_csvfp}")
        return _pt_load_save(new_csvfp, new_ptfp)

    if db_name in mongoc.list_database_names() and new_data_name in mongoc[db_name].list_collection_names():
        return export_load_save(mongoc[db_name][new_data_name].find_one().keys())

    if db_name not in mongoc.list_database_names() or data_name not in mongoc[db_name].list_collection_names():
        if not os.path.isfile(jsfp):
            if not os.path.isfile(zfp):
                print("retrieving from gdrive.")
                gdown.download('https://drive.google.com/uc?id=1lgvxFp7l67dyZEGmvkqeP4fMN5hAQQ-q', zfp, quiet=False)
            _runcmd(f"unzip {zfp} -d {data_dir}")
        _runcmd(f"mongoimport --db {db_name} --collection {data_name} --drop --file {_path(data_dir, f'{data_name}.json')} --numInsertionWorkers {os.cpu_count()} --writeConcern 1")

    transform_data(db:=mongoc[db_name], db[data_name], new_data_name, num_docs)
    return export_load_save(mongoc[db_name][new_data_name].find_one().keys())