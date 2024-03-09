from yaat.maester import ModelEntry, DataEntry

print(ModelEntry.from_json(ModelEntry('1','2', '3', '4', {1:1}).to_json()))
print(DataEntry('tickers.csv'))
