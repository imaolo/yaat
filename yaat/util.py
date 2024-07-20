import subprocess, select, requests

def fetchjson(url:str): return requests.get(url).json()
def killproc(proc:subprocess.Popen):
        proc.terminate()
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f"process {proc.args} didn't terminate gracefully, killing it...")
            proc.kill()
def runcmd(cmd:str):
    def ass(proc): assert proc.returncode is None or proc.returncode == 0 or proc.returncode == 128, f"Command failed - {cmd} \n\n returncode: {proc.returncode} \n\n stdout: \n {proc.stdout.read()} \n\n stderr: \n{proc.stderr.read()} \n\n"
    proc = subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    while True: # stream print
        ass(proc)
        if ready:=select.select([proc.stdout, proc.stderr], [], [], 0.1)[0]:
            if out:=ready[-1].readline(): print(out.strip()) # eh, only last one. if we miss some, whatever
        if proc.poll() is not None: break
    ass(proc)
