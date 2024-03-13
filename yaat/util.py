from dotenv import load_dotenv
from typing import Any, List
import subprocess, pprint, select, os, pickle

load_dotenv()

def path(*fp:str) -> str: return '/'.join(fp)
def exists(fp:str): return os.path.isdir(fp) or os.path.isfile(fp)
def getenv(key:str, default=None) -> Any: return os.getenv(key, default) if default is not None else os.environ[key]
def myprint(header:str, obj:Any): print(f"{'='*15} {header} {'='*15}"); pprint.pprint(obj)
def mkdirs(*args, mode=0o755, **kwargs): os.makedirs(*args, mode=mode, **kwargs)
def objsz(obj:Any) -> int: return len(pickle.dumps(obj))
def filesz(fp: str) -> int: return os.path.getsize(fp)
def siblings(fp:str) -> List[str]: return os.listdir(path(*fp.split('/')[:-1]))
def parent(fp:str) -> List[str]: return path(*fp.split('/')[-2])
def leaf(fp:str) -> str: return fp.split('/')[-1]
def read(fp:str) -> str:
    with open(fp, 'r') as f: return f.read()
def write(fp:str, data:str):
    with open(fp, 'w') as f: f.write(data)
def append(fp:str, data:str):
    with open(fp, 'a') as f: f.write(data)
def rm(fp:str):
     if os.path.isdir(fp): runcmd(f"rm -rf {fp}")
     if os.path.isfile(fp): os.remove(fp)
def runcmd(cmd:str):
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
def killproc(proc:Any, proc_name:str):
        print(f"shutting down process: {proc_name}")
        proc.terminate()
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f"process {proc_name} didn't terminate gracefully, killing it...")
            proc.kill()
        print(f"process {proc_name} shutdown")