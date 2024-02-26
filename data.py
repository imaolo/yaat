import gdown, os, subprocess, select, pprint, atexit, functools, pymongo, torch, pandas

# helpers
def path(*fp): return '/'.join(fp)
def myprint(header, obj):
    print(f"{'='*15} {header} {'='*15}")
    pprint.pprint(obj)
def runcmd(cmd:str):
    def myass(proc): assert proc.returncode is None or proc.returncode == 0, f"Command failed - {cmd} \n\n returncode: {proc.returncode} \n\n stdout: \n {proc.stdout.read()} \n\n stderr: \n{proc.stderr.read()} \n\n"
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
def pt_load_save(fp_from, fp_to):
    print(f"reading {fp_from} into {fp_to}")
    data = torch.tensor(pandas.read_json(fp_from, lines=True)['last'].to_numpy(), dtype=torch.float32)
    torch.save(data, fp_to) 
    return data

# configs
db_name = 'yaatdb1'
data_name = 'tickers'
new_data_name = 'eth_tickers' # just eth for now
data_dir = 'data'
db_dir = path(data_dir, 'db')
zfp = f"{(data_fp:=path(data_dir, data_name))}.zip"
jsfp = f"{data_fp}.json"
new_ptfp = f"{(new_data_fp:=path(data_dir, new_data_name))}.pt"
new_jsfp = f"{new_data_fp}.json"
num_docs = 483380

# caching :(
def clean_data(): runcmd(f"rm -rf {data_dir}")

def fetch_data() -> torch.Tensor:
    # make directories
    if not os.path.exists(data_dir): os.makedirs(data_dir)
    if not os.path.exists(db_dir): os.makedirs(db_dir)

    # caching :)
    if os.path.isfile(new_ptfp):
        print(f"loading from {new_ptfp}")
        return torch.load(new_ptfp)
    if os.path.isfile(new_jsfp): return pt_load_save(new_jsfp, new_ptfp)

    # start and connect to database and set its kill hook
    print("starting mongo")
    mongod = subprocess.Popen(['mongod', '--dbpath', db_dir], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert mongod.poll() is None
    mongoc = pymongo.MongoClient()
    print("mongo started")
    atexit.register(functools.partial(killproc, mongod, 'mongod'))

    # import to database
    if db_name not in mongoc.list_database_names() or data_name not in mongoc[db_name].list_collection_names():
        if not os.path.isfile(jsfp):
            if not os.path.isfile(zfp): # retrieve raw data
                print("retrieving from gdrive.")
                gdown.download('https://drive.google.com/uc?id=1lgvxFp7l67dyZEGmvkqeP4fMN5hAQQ-q', zfp, quiet=False)
            runcmd(f"unzip {zfp} -d {data_dir}")
        runcmd(f"mongoimport --db {db_name} --collection {data_name} --drop --file {path(data_dir, f'{data_name}.json')} --numInsertionWorkers {os.cpu_count()} --writeConcern 1")
    db = mongoc[db_name]
    data = db[data_name]

    # clean & transform
    if new_data_name not in db.list_collection_names():
        print("get bybit eth attr") # TODO - get more than just eth, doc.data unwind should start it
        data.aggregate([
            {'$set': {'data.ETH.bybit.epoch': {'$toLong': '$datetime'}}},
            {'$project': {'_id': 0, 'newDoc': '$data.ETH.bybit'}},
            {'$unset': ['newDoc.info', 'newDoc.timestamp']},
            {'$replaceRoot': {'newRoot': '$newDoc'}},
            {'$out': new_data_name}
        ])
        new_data = db[new_data_name]
        myprint(new_data_name, new_data.find_one())

        print("get set of attr keys")
        keys = set(list(new_data.aggregate([
            {'$project': {'kvarr': {'$objectToArray': '$$ROOT'}}},
            {'$unwind': '$kvarr'},
            {'$group': {
                '_id': None,
                'collkeys': {'$addToSet': '$kvarr.k'}
            }},
        ]))[0]['collkeys'])
        keys.remove('_id')
        myprint(f"{new_data_name} keys", keys)

        print("get set of null or dne attr keys")
        bad_keys = {}
        for k in keys:
            res = list(new_data.aggregate([
                {'$match': {'$or': [{k: {'$exists': False}}, {k: None}]}},
                {'$count': 'num_docs'}
            ]))
            if res: bad_keys[k] = res[0]['num_docs']
        myprint(f"{new_data_name} null or dne keys", bad_keys)

        print("unset bad keys")
        unset_operation = {key: "" for key in bad_keys.keys()}
        new_data.update_many({}, {'$unset': {key: "" for key in bad_keys.keys()}})
        myprint(f"{new_data_name} w/o bad keys", new_data.find_one())

        print("counting docuemnts.")
        assert db[new_data_name].count_documents({}) == num_docs
        print("documents counted.")

    # export from database
    runcmd(f"mongoexport --db {db_name} --collection {new_data_name} --out {new_jsfp}") 

    # load & return tensor
    return pt_load_save(new_jsfp, new_ptfp)