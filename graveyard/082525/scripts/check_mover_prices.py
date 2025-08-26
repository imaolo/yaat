
from yaat import State
from yaat.coingecko import CoinGecko, TopMoverResultDoc
from datetime import timedelta as td
import asyncio

from pprint import pprint

async def get_percent_change_hour(mover: dict) -> float | None:
    cg_data = await CoinGecko.historical_chart_range(mover['cid'], vs_currency='usd', **{'from':(now:=mover['created_at']).timestamp()},
                                                     to=(now + td(hours=1)).timestamp(), precision='10')

    try:
        start_price = cg_data['prices'][0][1]
        end_price = cg_data['prices'][-1][1]
        return ((end_price - start_price) / start_price)*100
    except:
        print('exception!')
        pprint(mover)
        pprint(cg_data)
        return None

async def main():
    await State.init_beanie()

    match_dur_stage = {'$match': {'query.duration': '1h'}}
    match_perc_change_stage = {'$match': {'percent_change': {'$gt': -100.0}}}
    match_perc_change_stage_2 = {'$match': {'percent_change': {'$lt': -5.0}}}
    match_market_cap_rank_stage = {'$match': {'market_cap_rank': {'$lt': 500}}}
    match_hour_later_stage_pos = {'$match': {'hour_later': {'$gt': -100}}}
    match_hour_later_stage_neg = {'$match': {'hour_later': {'$lt': -100}}}

    # await TopMoverResultDoc.aggregate([
    #     match_dur_stage,
    #     match_perc_change_stage,
    #     {'$out': (cn:='new_tmp_trash_collection')},
    # ]).to_list()

    # coll = State.client['yaatdb'][cn]
    # for doc in (await coll.find().to_list()):
    #     await coll.update_one(
    #         {"_id": doc["_id"]},
    #         {"$set": {'hour_later':await get_percent_change_hour(doc)}}
    #     )

    # pprint(await coll.aggregate([
    #     {'$sort': {'hour_later': 1}},
    #     {'$project': {'sign': {'$cond': [{ '$gt': ["$hour_later", 0] }, "positive", "negative"]}}},
    #     {'$group': {'_id': "$sign",'count': { '$sum': 1 }}}
    # ]).to_list())


    movers = await TopMoverResultDoc.aggregate([
        match_dur_stage,
        match_perc_change_stage,
        match_perc_change_stage_2,
        match_market_cap_rank_stage,
        {'$group': {
            '_id': {
                '$dateTrunc': {
                      'date': '$created_at',
                      'unit': 'minute'
                }
            },
            'items': {'$push': '$$ROOT'},
            'avg_market_cap_rank': {'$avg': 'market_cap_rank'}
        }}
    ]).to_list()

    new_movers_agg = []
    acc = 0
    for i in range(1, len(movers)):
        prev_movers_cid = [mover['cid'] for mover in movers[i-1]['items']]
        acc += len(movers[i]['items'])
        new_movers = [mover for mover in movers[i]['items'] if mover['cid'] in prev_movers_cid]
        new_movers_agg.extend(new_movers)

    print(len(new_movers_agg), " new movers. ", acc)
    for mover in new_movers_agg:
        mover['hour_later'] = await get_percent_change_hour(mover)

    coll = State.client['yaatdb'][cn:='new_tmp_trash_collection']
    await coll.drop()
    await coll.insert_many(new_movers_agg)

    pprint(await coll.aggregate([
        {'$sort': {'hour_later': 1}},
        {'$project': {'sign': {'$cond': [{ '$gt': ["$hour_later", 0] }, "positive", "negative"]}}},
        {'$group': {'_id': "$sign",'count': { '$sum': 1 }}}
    ]).to_list())
    pprint(await coll.aggregate([
        match_hour_later_stage_pos,
        match_hour_later_stage_neg
    ]).to_list())


    # pprint(new_movers_agg)
    # print(len(new_movers_agg))
    # print()

    # agg = await TopMoverResultDoc.aggregate([
    #     *setup_stages,
    #     {'$group': {
    #         '_id': None,
    #         'avg_market_cap_rank': {'$avg': 'market_cap_rank'}
    #     }}
    # ]).to_list()
    # print(agg)

asyncio.run(main())
