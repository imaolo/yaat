import subprocess, pprint, select, os, torch, pandas, gdown

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

def get_tickers():
    addext = lambda ext: 'data/tickers.'+ext
    anyexist = lambda *exts: any(os.path.isfile(addext(ext)) for ext in exts)
    if not os.path.isdir('data'): os.makedirs('data')
    if not anyexist('pt', 'csv', 'zip'): gdown.download('https://drive.google.com/uc?id=11Jt2PpKcKZLaifZXjlCAVpSqdh4VG8Vt', addext('zip'), quiet=False) 
    if not anyexist('pt', 'csv'): runcmd(f"unzip {addext('zip')}")
    if anyexist('pt'): tickers = torch.load(addext('pt'))
    else:
        df = pandas.read_csv(addext('csv'))
        tickers = {sym: torch.tensor(df[df['symbol'] == sym]['last'].to_numpy(), dtype=torch.float32) for sym in df['symbol'].unique()}
        torch.save(tickers, addext('pt'))
    return tickers