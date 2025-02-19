

            # TIME_SERIES_INTRADAY
# myprint("stocks example", self.call_alpha(function='TIME_SERIES_INTRADAY', interval='60min', symbol='SPY'))
# =============== stocks example ===============
# {'Meta Data': {'1. Information': 'Intraday (60min) open, high, low, close '
#                                  'prices and volume',
#                '2. Symbol': 'SPY',
#                '3. Last Refreshed': '2024-05-13 20:00:00',
#                '4. Interval': '60min',
#                '5. Output Size': 'Compact',
#                '6. Time Zone': 'US/Eastern'},
#  'Time Series (60min)': {'2024-05-06 06:00:00': {'1. open': '512.7300',
#                                                  '2. high': '513.3600',
#                                                  '3. low': '512.6900',
#                                                  '4. close': '513.2700',
#                                                  '5. volume': '45653'},
#                          '2024-05-06 07:00:00': {'1. open': '513.2400',
#                                                  '2. high': '513.2500',
#                                                  '3. low': '512.6000',
#                                                  '4. close': '513.1000',
#                                                  '5. volume': '142978'},
#                          '2024-05-06 08:00:00': {'1. open': '512.3090',



        # myprint("missing ticks", miing_ticks)

        # TODO - get symbols and datetimes that are not filled in

        #     # TODO retrieve and insert!!!
        # [{'_id': datetime.datetime(1, 1, 1, 0, 0),
        # 'count': 29,
        # 'symbols': [{'datetime': datetime.datetime(1, 1, 1, 0, 0), 'symbol': 'SPY'},
        #             {'datetime': datetime.datetime(1, 1, 1, 0, 0), 'symbol': 'SPY'}]

        # # verify the results
        # for tbt in tb_ticks_unfilled:
        #     syms_dates = tbt['syms_dates']
        #     tmp_syms = []
        #     for sds in tbt['syms_dates']:
        #         tmp_syms.append(sds['symbol'])
        #         assert tbt['_id'] == sds['datetime'], f"bucket and ticker datetimes must be equal! {tbt}"
        #     assert len(tmp_syms) == len(set(tmp_syms)), f"no repeats! {tmp_syms}"

        # # time_buckets = pd.date_range(start=start, end=end, freq=f'{interv_min}T')




        # # flatten into syms_dates
        # syms_dates = []
        # for tbt in tb_ticks_unfilled:
        #     for sds in tbt['syms_dates']:
        #         syms_dates.append((sds['datetime'].astimezone(Maester.tz), sds['symbol']))
        #     # assert len(tmp_syms) == len(set(tmp_syms)), f"no repeats! {tmp_syms}"
        
        # # get the syms and tickers that need to be fetched
        # all_tbs = pd.DataFrame(pd.date_range(start=start, end=end, freq=f'{interv_min}min', tz=Maester.tz), columns=['datetime'])
        # print(all_tbs)
        # # all_tbs_syms_df = pd.DataFrame([(dt, sym) for dt in all_tbs for sym in syms], columns=['datetime', 'symbol'])
        # # syms_dates_df = pd.DataFrame(syms_dates, columns=['datetime', 'symbol'])
        # # print(all_tbs_syms_df)
        # # merged = pd.merge(syms_dates_df, all_tbs_syms_df, on=['datetime', 'symbol'], how='left', indicator=True)
        # # print(merged)

        # # get the missing symbols and datetimes
        # missing_dates_syms = {}
        # for c in tb_ticks:
        #     dt = c['_id']
        #     syms_dates = c['syms_dates']

        #     syms_there = []
        #     for sds in syms_dates:
        #         assert sds['datetime'] == dt, f"{sds['datetime']}, {sds['datetime']}, {sds['symbol']}"
        #         syms_there.append(sds['symbol'])

        #     # there should be no repeat entries in this bucket
        #     assert len(set(syms)) == len(syms), syms

        #     if set(syms_there) != syms: missing_dates_syms[dt] = syms - set(syms_there)
                



                


            # all documents should have the exact datetime
            


        

        # Maester.tickers_coll.insert_one({
        #     'symbol': 'ETH',
        #     'datetime': datetime(1,1,1),
        #     'open': 1.0,
        #     'close': 1.0,
        #     'high': 1.0,
        #     'low': 1.0,
        #     'volume': None
        # })
            

        # we have stocks, currencies, and crypto currencies

        # myprint("stocks example", self.call_alpha(function='TIME_SERIES_INTRADAY', interval='60min', symbol='SPY'))
        # myprint("currency example", self.call_alpha(function='FX_INTRADAY', interval='60min', from_symbol='EUR', to_symbol='USD'))
        # myprint("crypto currency example", self.call_alpha(function='CRYPTO_INTRADAY', interval='60min', symbol='ETH', market='USD'))

        # TODO - need to figure out how to reuse collections and recognize a continuance of a scrape job
        # this will presumably be based on which symbols we are recieving
        
        # the issue is overlap. What happens if I remove one ticker from the list

        # TODO - define the collection (name, schema, etc)
        # TODO - determine when to start (based on what is in the database already)
        # TODO - retrieve data until [start, stop] is collected



# TIME_SERIES_INTRADAY
# myprint("stocks example", self.call_alpha(function='TIME_SERIES_INTRADAY', interval='60min', symbol='SPY'))
# =============== stocks example ===============
# {'Meta Data': {'1. Information': 'Intraday (60min) open, high, low, close '
#                                  'prices and volume',
#                '2. Symbol': 'SPY',
#                '3. Last Refreshed': '2024-05-13 20:00:00',
#                '4. Interval': '60min',
#                '5. Output Size': 'Compact',
#                '6. Time Zone': 'US/Eastern'},
#  'Time Series (60min)': {'2024-05-06 06:00:00': {'1. open': '512.7300',
#                                                  '2. high': '513.3600',
#                                                  '3. low': '512.6900',
#                                                  '4. close': '513.2700',
#                                                  '5. volume': '45653'},
#                          '2024-05-06 07:00:00': {'1. open': '513.2400',
#                                                  '2. high': '513.2500',
#                                                  '3. low': '512.6000',
#                                                  '4. close': '513.1000',
#                                                  '5. volume': '142978'},
#                          '2024-05-06 08:00:00': {'1. open': '512.3090',


# myprint("currency example", self.call_alpha(function='FX_INTRADAY', interval='60min', from_symbol='EUR', to_symbol='USD'))
# =============== currency example ===============
# {'Meta Data': {'1. Information': 'FX Intraday (60min) Time Series',
#                '2. From Symbol': 'EUR',
#                '3. To Symbol': 'USD',
#                '4. Last Refreshed': '2024-05-14 01:00:00',
#                '5. Interval': '60min',
#                '6. Output Size': 'Compact',
#                '7. Time Zone': 'UTC'},
#  'Time Series FX (60min)': {'2024-05-07 21:00:00': {'1. open': '1.07562',
#                                                     '2. high': '1.07566',
#                                                     '3. low': '1.07529',
#                                                     '4. close': '1.07540'},
#                             '2024-05-07 22:00:00': {'1. open': '1.07529',
#                                                     '2. high': '1.07564',
#                                                     '3. low': '1.07516',
#                                                     '4. close': '1.07559'},
#                             '2024-05-07 23:00:00': {'1. open': '1.07553',

# myprint("crypto currency example", self.call_alpha(function='CRYPTO_INTRADAY', interval='60min', symbol='ETH', market='USD'))
# =============== crypto currency example ===============
# {'Meta Data': {'1. Information': 'Crypto Intraday (60min) Time Series',
#                '2. Digital Currency Code': 'ETH',
#                '3. Digital Currency Name': 'Ethereum',
#                '4. Market Code': 'USD',
#                '5. Market Name': 'United States Dollar',
#                '6. Last Refreshed': '2024-05-14 00:00:00',
#                '7. Interval': '60min',
#                '8. Output Size': 'Compact',
#                '9. Time Zone': 'UTC'},
#  'Time Series Crypto (60min)': {'2024-05-09 21:00:00': {'1. open': '3021.24000',
#                                                         '2. high': '3029.44000',
#                                                         '3. low': '3015.72000',
#                                                         '4. close': '3020.11000',
#                                                         '5. volume': 1128},
#                                 '2024-05-09 22:00:00': {'1. open': '3020.11000',
#                                                         '2. high': '3058.43000',
#                                                         '3. low': '3016.31000',
#                                                         '4. close': '3053.60000',
#                                                         '5. volume': 4099},
#                                 '2024-05-09 23:00:00': {'1. open': '3053.55000',



# # get tickers time buckets that are not filled
# tb_ticks_unfilled = list(Maester.tickers_coll.aggregate([
#     {'$match': {
#         'symbol': {'$in': syms},
#         # 'datetime': {'$gte': datetime(1, 1, 1, 1), '$lte': datetime(2, 1, 1, 1)}
#     }},
#     {'$sort' : {'datetime':1}},
#     {'$project': {
#         'dt_bucket': {
#             '$dateTrunc': {
#                 'date': '$datetime',
#                 'unit': 'minute',
#                 'binSize': interv_min
#             }
#         },
#         'datetime': 1,
#         'symbol': 1
#     }},
#     {'$group': {
#         '_id': '$dt_bucket',
#         'count': {'$sum': 1},
#         'syms_dates': {'$push': {
#             'symbol': '$symbol',
#             'datetime': '$datetime'}}
#     }},
#     # {'$match': {'count': {'$lt': len(syms)}}},
# ]))
# myprint('eh', tb_ticks_unfilled)



        # ### Testing ###
        # if False:
        #     Maester.tickers_coll.drop()

        #     Maester.tickers_coll.insert_one({
        #         'symbol': 'ETH',
        #         'datetime': datetime.now(tz=Maester.tz),
        #         'open': 1.0,
        #         'close': 1.0,
        #         'high': 1.0,
        #         'low': 1.0,
        #         'volume': None
        #     })
        #     Maester.tickers_coll.insert_one({
        #         'symbol': 'SPY',
        #         'datetime': datetime.now(tz=Maester.tz),
        #         'open': 1.0,
        #         'close': 1.0,
        #         'high': 1.0,
        #         'low': 1.0,
        #         'volume': None
        #     })
        # ### end testing ###