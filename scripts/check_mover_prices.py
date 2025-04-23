
from yaat import State
from yaat.coingecko import CoinGecko, TopMoverResultDoc
from datetime import timedelta as td
import asyncio

from pprint import pprint

async def get_percent_change_hour(mover: dict) -> float:
    cg_data = await CoinGecko.historical_chart_range(mover['cid'], vs_currency='usd', **{'from':(now:=mover['created_at']).timestamp()},
                                                     to=(now + td(hours=1)).timestamp(), precision='10')

    start_price = cg_data['prices'][0][1]
    end_price = cg_data['prices'][-1][1]
    return ((end_price - start_price) / start_price)*100

async def main():
    await State.init_beanie()
    movers = await TopMoverResultDoc.aggregate([
        {'$match': {'query.duration': '1h'}},
        {'$match': {'percent_change': {'$gt': 20}}},
        {'$sort': {'created_at': 1}},
        {'$group': {
            '_id': {
                '$dateTrunc': {
                      'date': '$created_at',
                      'unit': 'minute'
                }
            },
            'items': {'$push': '$$ROOT'}
        }}
    ]).to_list()

    new_movers_agg = []
    for i in range(1, len(movers)):
        prev_movers_cid = [mover['cid'] for mover in movers[i-1]['items']]
        new_movers = [mover for mover in movers[i]['items'] if mover['cid'] not in prev_movers_cid]
        new_movers_agg.extend(new_movers)

    for mover in new_movers_agg:
        mover['hour_later'] = await get_percent_change_hour(mover)

    pprint(new_movers_agg)
    print(len(new_movers_agg))

asyncio.run(main())
