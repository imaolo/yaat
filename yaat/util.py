import subprocess, requests, dotenv, os

dotenv.load_dotenv()

def getenv(key:str, default=0): return type(default)(os.getenv(key, default))
def fetchjson(url:str): return requests.get(url).json()
def killproc(proc:subprocess.Popen):
        proc.terminate()
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f"process {proc.args} didn't terminate gracefully, killing it...")
            proc.kill()


DB_PW, DB_IP = getenv('DB_PW'), getenv('DB_IP')