from dotenv import load_dotenv
from typing import Any
import subprocess, pprint, select, os

load_dotenv()

def path(*fp:str) -> str: return '/'.join(fp)
def getenv(key:str, default=None, req=True) -> Any: return os.environ[key] if req else os.getenv(key, default)
def myprint(header:str, obj:Any): print(f"{'='*15} {header} {'='*15}"); pprint.pprint(obj)
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