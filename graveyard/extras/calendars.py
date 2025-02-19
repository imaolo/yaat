from yaat.util import myprint
from yaat.maester import TimeRange, Maester, DATE_FORMAT, TIME_FORMAT
from pymongo import UpdateOne
from datetime import timedelta, datetime, time, date
import sys, pandas as pd


print(Maester.alpha_get_times(60))

sys.exit()

# start_tr = '2000-01-1'
# end_tr = '2018-02-1'
# mins_per_day = 2

# # create times
# s = datetime.combine(datetime.today(), time())
# times = [(s + timedelta(minutes=i)).time() for i in range(mins_per_day)]


m = Maester()
# myprint("before mine", list(m.tickers.find({}, projection={'timestamp': True})))


print(m.tickers.count_documents({}))
m.mine_alpha(date(2020, 1, 1), date(2020, 1, 10), 'XLK', 1)
print(m.tickers.count_documents({}))





sys.exit()
res = Maester.call_alpha(function='TIME_SERIES_INTRADAY', symbol
='IBM', interval=f"{60}min", extended_hours='false', month='2022-01')
timestamps = pd.DatetimeIndex(res['Time Series (60min)'].keys()).tz_localize(res['Meta Data']['6. Time Zone']).tz_convert('UTC')
times = pd.unique(timestamps.time)
print(times)
# myprint("res", res)

sys.exit()

# setup maester
m = Maester()
# m.timestamps.delete_many({})

# create time range
tr = TimeRange(start_tr, end_tr, times)

# get existing timestamps
existing_ts_docs = list(m.timestamps.aggregate([
    {'$addFields':{
        'just_date': {'$dateToString': {'format': DATE_FORMAT, 'date': '$timestamp'}},
        'just_time': {'$dateToString': {'format': TIME_FORMAT, 'date': '$timestamp'}}
    }},
    {'$match': {
        'just_time': {'$in': tr.times},
        'just_date': {'$in': tr.days.strftime(DATE_FORMAT).to_list()}
    }},
    {'$project': {'just_time': 0, 'just_date': 0}}
]))
print(existing_ts_docs[0])
existing_ts = pd.DatetimeIndex(pd.DataFrame(existing_ts_docs)['timestamp'])
missing_ts = tr.timestamps.difference(existing_ts)

# insert timestamps
updates = [UpdateOne((doc:={'timestamp': dt}), {'$setOnInsert': doc}, upsert=True) for dt in missing_ts]
result = m.timestamps.bulk_write(updates)
print(result.inserted_count + len(result.upserted_ids))
print(m.timestamps.count_documents({}))

sys.exit()



# import pandas_market_calendars as mcal
# from yaat.maester import DateRange, Maester, TimeRange
# from yaat.util import myprint
# from yaat.miner import Miner
# import atexit, functools, pymongo.errors as mongoerrs, pandas as pd
# from datetime import datetime, date, time
# from zoneinfo import ZoneInfo
# import sys
# from yaat.maester import get_exchange_timestamps
# from pymongo import MongoClient, UpdateOne, InsertOne
# from pathlib import Path

# # m = Maester()
# dr = DateRange(datetime(2000, 1, 1), datetime(2000, 1, 2), 1)
# times = set([dt.time() for dt in dr.intervals])

# ts 

# tr = TimeRange(date(2000, 1, 1), date(2020, 2, 1), times)
# res = m.fill_timestamps(tr)

# sys.exit()


# p = Path('yaatdb_local')
# print(p)

# p = Path(p)
# print(p)

# sys.exit()

# print(str(time(11, tzinfo=ZoneInfo('America/Los_Angeles'))))



# print(str(time(11, tzinfo=ZoneInfo('UTC'))))


# print(datetime(2000, 1, 1, tzinfo=ZoneInfo('America/Los_Angeles')))



# m = Maester(None)
# m.timestamps.delete_many({})
# m.timestamps.insert_one({'timestamp': datetime(2000, 1, 1, tzinfo=ZoneInfo('America/Los_Angeles'))})
# print(m.timestamps.find_one({}))



sys.exit()
dr = DateRange(datetime(2000, 1, 1), datetime(2000, 1, 2), 1)
times = set([dt.time() for dt in dr.intervals])

m.fill_timestamps(date(2000, 1, 1), date(2020, 2, 1), times)




sys.exit()



# miner = Miner(None)
# res = miner.call_alpha(function='TIME_SERIES_INTRADAY', symbol='IBM', interval='60min', extended_hours="false", month='2022-01', outputsize='full')
# myprint("yer", res)

# sys.exit()

# get the times
dr = DateRange(datetime(2000, 3, 16), datetime(2000, 3, 17), 60)
ints = dr.intervals
print(ints)

miner = Miner(None)
res = miner.call_alpha(function='TIME_SERIES_INTRADAY', symbol='IBM', interval='60min', extended_hours="false", month='2022-01', outputsize='full')
myprint("yer", res)

sys.exit()

start_date = '2000-03-16'
end_date = '2023-03-18'
nyse = mcal.get_calendar('NYSE')
trading_days = nyse.schedule(start_date, end_date)
# print("trading days index")
# print(trading_days.index)

trading_days_repeated = trading_days.index.repeat(len(times))
# print("trading days index repeated")
# print(trading_days_repeated)

times_repeated = pd.to_timedelta(times * len(trading_days))

all_datetimes = trading_days_repeated + times_repeated

print(len(all_datetimes))


print(get_exchange_timestamps(start_date, end_date, times).memory_usage(deep=True)/1e6)

# word ( :) )
sys.exit()





trading_days = pd.to_datetime(trading_days['market_open']).dt.date.repeat(len(times))
times_repeated = pd.to_timedelta(times * len(trading_days))
print(trading_days.head(10))

# Combine dates with times
all_datetimes = trading_days + times_repeated

sys.exit()

times = ['10:00:00', '11:00:00']

# Generate datetime index for each trading day at specified times
all_datetimes = pd.DatetimeIndex([])
date_strings = trading_days.index.date.astype(str)


# Generate datetime index for each trading day at specified times
all_datetimes = pd.DatetimeIndex([])
for time in times:
    # Efficiently combine all dates with a single time, then convert to datetime
    datetime_strs = trading_days.index.date.astype(str) + ' ' + time
    datetime_series = pd.to_datetime(datetime_strs)
    all_datetimes = all_datetimes.union(datetime_series)


# Display the resulting datetime objects
print(all_datetimes)

# # sched = nyse.schedule(start_date='2024-2-6', end_date='2024-2-6')

# # dr = mcal.date_range(schedule=sched, frequency='60min').tz_convert('US/Eastern')
# # print(dr)

# miner = Miner(None)
# res = miner.call_alpha(function='TIME_SERIES_INTRADAY', symbol='IBM', interval='60min', extended_hours="false", month='2022-01', outputsize='full')
# myprint("yer", res)

