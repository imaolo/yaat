## Install
```bash
python -m pip install -e . 
```

## Testing
```bash
python -m pip install -e '.[testing]'
python -m pytest test/ 
```

## Get Original Data
yaat.py has all the information it needs to download the
correct data. It downloads the restructured and cleaned
form of the old data. If you want to download the old data
and run the restructure script... it is slow. More notes in the file.
```bash
python get_new_data.py
```
#### note: get_new_data.py needs mongo
```bash
pip install pymongo
```
https://www.mongodb.com/docs/manual/installation/
##### macos

```bash
brew tap mongodb/brew
brew install mongodb-community
```
