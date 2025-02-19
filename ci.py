import os, subprocess

SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_HOST = os.getenv('SSH_HOST')
SSH_KEY = os.getenv('SSH_KEY')
GH_PAT = os.getenv('GH_PAT')

if not SSH_USERNAME or not SSH_KEY or not SSH_HOST or not GH_PAT:
    raise RuntimeError("SSH_USERNAME, SSH_KEY, SSH_HOST, and GH_PAT required")

def runcmd(cmd:str) -> None | str:
    print(cmd)
    subprocess.run(cmd, check=True, shell=True)

if __name__ == "__main__":
    runcmd('python -m pytest tests')
    ssh_cmd = "cd test-cicd && "
    ssh_cmd += f"git pull https://imaolo:{GH_PAT}@github.com/imaolo/test-cicd && "
    ssh_cmd += "docker-compose up --build --force-recreate --remove-orphans -d"
    runcmd('echo "$SSH_KEY" > ~/id_rsa.pem')
    os.chmod(os.path.expanduser('~/id_rsa.pem'), 0o600)
    runcmd(f'ssh -o StrictHostKeyChecking=no -i ~/id_rsa.pem {SSH_USERNAME}@{SSH_HOST} "{ssh_cmd}"')