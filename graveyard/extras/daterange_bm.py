import pandas as pd
from datetime import datetime
from yaat.maester import DateRange
import time

entire_range = DateRange(datetime(2000, 1, 1), datetime(2020, 1, 1), 1)
entire_range_idx = pd.date_range(start=entire_range.start, end=entire_range.end, freq=f'{entire_range.freq_min}min')



existing_range = DateRange(datetime(2000, 1, 1), datetime(2020, 1, 1), 15)
existing_range_idx = pd.date_range(start=existing_range.start, end=existing_range.end, freq=f'{existing_range.freq_min}min')


# these are the only things available (no existing range idx)
existing_range_ser = pd.Series(existing_range_idx, name='datetime')
entire_range_idx = pd.date_range(start=entire_range.start, end=entire_range.end, freq=f'{entire_range.freq_min}min')

# how to find the missing intervals in the

# simulated
def get_existing_intervals(): return [{'datetime': datetime(2000, 1, 1)}, {'datetime': datetime(2020, 1, 1)}]

entire_range = DateRange(datetime(2000, 1, 1), datetime(2020, 1, 1), 1)
existing_intervals = get_existing_intervals()
existing_intervals = [doc['datetime'] for doc in existing_intervals]


entire_range_idx = pd.date_range(start=entire_range.start, end=entire_range.end, freq=f'{entire_range.freq_min}min')
existing_range_idx = pd.DatetimeIndex(existing_intervals)


print(len(entire_range_idx))
print(len(existing_range_idx))
diff = entire_range_idx.difference(existing_range_idx)
print(len(diff))



# what is the most efficient way to find the missing intervals? 
# we dont have to use pandas. Consider all solutions





start_t = time.process_time()
diff = entire_range_idx.difference(existing_range_idx)
print('elaps ', time.process_time() - start_t)

# start_t = time.process_time()
# result = pd.merge(pd.Series(index=int1), pd.Series(index=int2), left_index=True, right_index=True, how='outer', indicator=True)
# missing = result[result['_merge'] == 'left_only']
# print("elaps ", time.process_time() - start_t)


# now I want to join existing and int1, and use the indicator method to find which intervals are missing



