import subprocess, requests

def fetchjson(url:str): return requests.get(url).json()
def killproc(proc:subprocess.Popen):
        proc.terminate()
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f"process {proc.args} didn't terminate gracefully, killing it...")
            proc.kill()
