sudo apt-get update -y
sudo apt-get install python3.12 -y
sudo apt-get install python3.12-venv -y
python3.12 -m venv .venv
. .venv/bin/activate
pip install -e .