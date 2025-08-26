from yaat.state import State
from yaat.coingecko import TopMoverResultDoc, CoinGecko
from datetime import timedelta, datetime
import asyncio

async def main():
    await State.init_beanie()

    TopMoverResultDoc.doc_args.db_updateable = True

    hour = timedelta(hours=1.0)
    hour_2 = timedelta(hours=2.0)
    async for doc in TopMoverResultDoc.find(TopMoverResultDoc.hour_later_change == None):

        diff = datetime.now() - doc.created_at
        if diff > hour:
            to = doc.created_at + timedelta(hours=1.0)
            cg_data = await CoinGecko.historical_chart_range(doc.cid, vs_currency='usd', **{'from':(to-hour_2).timestamp()}, to=to.timestamp(), precision='10')
            try: _, hour_later_price = cg_data['prices'][-1]
            except:
                print("except!")
                print(doc)
            doc.hour_later_change = ((hour_later_price - doc.usd)/doc.usd)*100
            await doc.save()
            print(doc.symbol, " : ", doc.hour_later_change)
        else:
            print('skipped: ', doc.symbol)

    TopMoverResultDoc.doc_args.db_updateable = False

asyncio.run(main())