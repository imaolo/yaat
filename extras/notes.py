# graveyard

# exist_dataset = maester.datasets.find_one({'name':args.name})
# if exist_dataset is not None: raise RuntimeError(f"dataset {args.name} already exists")

# ohlcvs = list(maester.candles1min.aggregate([
#     {'$match': {'ticker': {'$in': args.tickers}}}
# ]))

# pprint(ohlcvs)

# maester.candles1min.database.command('collMod', maester.candles1min.name, validator={})
# maester.candles1min.drop_indexes()
# maester.candles1min.update_many({}, {'$rename': { 'window_start': 'date' } })
    