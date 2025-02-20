import os, subprocess

SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_HOST = os.getenv('SSH_HOST')
SSH_KEY = os.getenv('SSH_KEY')
GH_PAT = os.getenv('GH_PAT')
DEPLOY = os.getenv('DEPLOY')

if DEPLOY and (not GH_PAT or not SSH_KEY or not SSH_HOST or not SSH_USERNAME):
    raise RuntimeError("Deploy requires GH_PAT, SSH_KEY, SSH_HOST, and SSH_USERNAME")

def runcmd(cmd:str) -> None | str:
    print(cmd)
    subprocess.run(cmd, check=True, shell=True)

if __name__ == "__main__":
    runcmd('python -m pytest tests')
    if DEPLOY is not None:
        ssh_cmd = "cd yaat && "
        ssh_cmd += f"git pull https://imaolo:{GH_PAT}@github.com/imaolo/yaat && "
        ssh_cmd += "docker-compose up --build --force-recreate --remove-orphans -d"
        runcmd('echo "$SSH_KEY" > ~/id_rsa.pem')
        os.chmod(os.path.expanduser('~/id_rsa.pem'), 0o600)
        runcmd(f'ssh -o StrictHostKeyChecking=no -i ~/id_rsa.pem {SSH_USERNAME}@{SSH_HOST} "{ssh_cmd}"')