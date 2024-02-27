import subprocess, pprint, select, atexit, functools

def path(*fp): return '/'.join(fp)

def myprint(header, obj):
    print(f"{'='*15} {header} {'='*15}")
    pprint.pprint(obj)

def runcmd(cmd):
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

def killproc(proc, proc_name):
        print(f"shutting down process: {proc_name}")
        proc.terminate()
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f"process {proc_name} didn't terminate gracefully, killing it...")
            proc.kill()
        print(f"process {proc_name} shutdown")

def conn_db(dir):
    import pymongo, pymongo.errors as mongoerrs
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