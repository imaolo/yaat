import pymongo

#### this file is mostly notes and is not valid python

# nohup python3.11 test.py > test.log 2>&1 &

# db.createUser({ user: "Earl", pwd: "fda#fDASc3.!!" , roles: [{ role: "dbOwner", db: "yaatdb"}] })
# this is js
db.createUser({
  user: "Earl",
  pwd: "pink-Flamingo1317",  // Choose a strong password
  roles: [{ role: "root", db: "admin" }]
})

# get the yaat db
coll = pymongo.MongoClient('localhost:27017')['yaatdb']['candles1min']

num = coll.aggregate([
    {'$match':{
        'ticker': {'$in': ['SMB', 'SMBC', 'SMBK']}
        {}
    }},
    {'$count': 'num'}
])

print(list(num))
