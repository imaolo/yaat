from dotenv import load_dotenv
from typing import Any, List, Dict, Type
import subprocess, pprint, select, os, pickle, json, time


def getenv(key:str, default=None) -> Any: return os.getenv(key, default) if default is not None else os.environ[key]

load_dotenv()
DEBUG = getenv("DEBUG", 0)
ROOT = getenv('ROOT', "data")

def path(*fp:str) -> str: return '/'.join(fp)
def gettypes(d: Dict[Any, Any], types:List[Type]) -> Dict[Any, Any]: return {k:v for k, v in d.items() if isinstance(v, types)}
def exists(fp:str): return os.path.isdir(fp) or os.path.isfile(fp)
def myprint(header:str, obj:Any=None):
    print(f"{'='*15} {header} {'='*15}")
    if obj: pprint.pprint(obj)
def gettime() -> int: return int(time.perf_counter_ns())
def mkdirs(*args, mode=0o755, **kwargs): os.makedirs(*args, mode=mode, **kwargs)
def objsz(obj:Any) -> int: return len(pickle.dumps(obj))
def filename(fp:str) -> str: return fp.split('/')[-1]
def filesz(fp: str) -> int: return os.path.getsize(fp)
def siblings(fp:str) -> List[str]: return os.listdir(path(*fp.split('/')[:-1]))
def children(fp:str) -> List[str]: return os.listdir(fp)
def serialize(obj:Any) -> bytes: return pickle.dumps(obj)
def construct(obj:bytes) -> Any: return pickle.loads(obj)
def dict2str(d:Dict) -> str: return json.dumps(d)
def str2dict(str:str) -> Dict: return json.loads(str)
def parent(fp:str) -> List[str]: return fp.split('/')[-2]
def leaf(fp:str) -> str: return fp.split('/')[-1]
def read(fp:str, mode='r') -> str | bytes:
    with open(fp, mode) as f: return f.read()
def write(fp:str, data:str | bytes, mode='w'):
    with open(fp, mode) as f: f.write(data)
def rm(fp:str):
     if os.path.isdir(fp): runcmd(f"rm -rf {fp}")
     if os.path.isfile(fp): os.remove(fp)
def runcmd(cmd:str):
    def ass(proc): assert proc.returncode is None or proc.returncode == 0, f"Command failed - {cmd} \n\n returncode: {proc.returncode} \n\n stdout: \n {proc.stdout.read()} \n\n stderr: \n{proc.stderr.read()} \n\n"
    if DEBUG: print(f"running command: {cmd}")
    proc = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    while True: # stream print
        ass(proc)
        if ready:=select.select([proc.stdout, proc.stderr], [], [], 0.1)[0]:
            if out:=ready[-1].readline(): print(out.strip()) # eh, only last one. if we miss some, whatever
        if proc.poll() is not None: break
    ass(proc)
    if DEBUG: print(f"command succeeded: {cmd}")
def killproc(proc:Any, proc_name:str):
        print(f"shutting down process: {proc_name}")
        proc.terminate()
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f"process {proc_name} didn't terminate gracefully, killing it...")
            proc.kill()
        print(f"process {proc_name} shutdown")

class TypeDict(dict):
    def __getitem__(self, key:Type[Any]) -> Any:
        item = next((v for k, v in self.items() if issubclass(key, k)), None)
        if not item: raise AttributeError(key)
        return item