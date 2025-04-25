from yaat.state import State
from yaat.coingecko import CoinGecko, TopMoverResultDoc
from datetime import datetime as dt, timedelta as td
from typing import Any
from pprint import pprint
import asyncio, numpy as np, pandas as pd

from scipy.stats.mstats import winsorize

window_size=2
train_size=12000
pred_size=4000
cushion_len = td(minutes=5)
window_len = td(minutes=15)

async def get_percent_change_hour(mover: TopMoverResultDoc) -> float | None:
    cg_data = await CoinGecko.historical_chart_range(mover.cid, vs_currency='usd', **{'from':(now:=mover.created_at).timestamp()},
                                                     to=(now + td(hours=2)).timestamp(), precision='10')

    try:
        start_price = cg_data['prices'][0][1]
        end_price = cg_data['prices'][-1][1]
        return ((end_price - start_price) / start_price)*100
    except:
        print('exception!')
        print(mover)
        pprint(cg_data)
        return None

async def get_training_point(doc: TopMoverResultDoc) -> dict | None:
    times = [doc.created_at]
    or_conds = []
    for _ in range(window_size-1):
        times.append(times[-1]-window_len)
        or_conds.append({'$and': [
            {'created_at': {'$gte': times[-1] - cushion_len}},
            {'created_at': {'$lte': times[-1] + cushion_len}}
        ]})

    prev = await TopMoverResultDoc.aggregate([{'$match': {'$or': or_conds, 'cid': doc.cid}}]).to_list()
    prev = list(map(lambda d: TopMoverResultDoc(**d), prev))
    training_set = {times[0]: doc}
    curr_prev_idx = 0
    for time in times[1:]:
        if (curr_prev_idx < len(prev)) and ((time - cushion_len) < prev[curr_prev_idx].created_at < (time + cushion_len)):
            training_set[time] = prev[curr_prev_idx]
            curr_prev_idx += 1
        else:
            training_set[time] = None

    X: list[TopMoverResultDoc | None] = training_set.values()
    Y: float = await get_percent_change_hour(doc)

    if Y is not None:
        def get_training_fields(i, x:TopMoverResultDoc | None) -> dict[str, Any]:
            if x:
                return {k+'-'+str(i): v for k, v in x.model_dump().items()}
            else:
                return {k+'-'+str(i): np.nan for k in TopMoverResultDoc.model_fields.keys()}

        return {
            'percent_change': Y,
            **{k:v for i, x in enumerate(X) for k, v in get_training_fields(i, x).items()}
        }
    
def create_df(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df = df.drop(columns=df.select_dtypes(exclude=['number']).columns)
    df = df.dropna(axis=1, how='all')  # drop all-NaN columns
    df = df.drop(columns=[col for col in df.columns if 'usd-' in col])
    def winsorize_series(s: pd.Series, limits=(0.05, 0.05)):
        winsorized = winsorize(s.to_numpy(), limits=limits)
        return pd.Series(np.asarray(winsorized), index=s.index)
    return df.apply(winsorize_series)

async def get_set(agg: Any) -> pd.DataFrame:
    ret = []
    i = 0
    async for doc in agg:
        i+=1
        if not (i % 50):
            print(i)
        point = await get_training_point(doc)
        if point:
            ret.append(point)
    return create_df(ret)

def get_x_y_from_df(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    return df.drop(columns='percent_change'), df['percent_change']
    
async def get_x_y_from_agg(agg: Any) -> tuple[pd.DataFrame, pd.Series]:
    return get_x_y_from_df(await get_set(agg))

async def train() -> tuple[Any, Any, Any]:
    X, y = await get_x_y_from_agg(TopMoverResultDoc.find({'query.duration': '1h'}).sort(('created_at',1),).skip(window_size).limit(train_size))

    import matplotlib.pyplot as plt
    import seaborn as sns

    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.preprocessing import RobustScaler

    x_scaler = RobustScaler()
    X_scaled = x_scaler.fit_transform(X)
    y_scaler = RobustScaler()
    y_scaled = y_scaler.fit_transform(y.values.reshape(-1, 1)).ravel()

    model = HistGradientBoostingRegressor()
    model.fit(X_scaled, y_scaled)

    return model, x_scaler, y_scaler

async def pred(model: Any, x_scaler: Any, y_scaler: Any):
    X, y = await get_x_y_from_agg(TopMoverResultDoc.find({'query.duration': '1h'}).sort(('created_at', 1),).skip(window_size+train_size).limit(pred_size))

    X_scaled = x_scaler.transform(X)
    y_scaled = y_scaler.transform(y.values.reshape(-1, 1)).ravel()

    preds = model.predict(X_scaled)

    from sklearn.metrics import mean_squared_error, r2_score

    mse = mean_squared_error(y_scaled, preds)
    r2 = r2_score(y_scaled, preds)

    print(f"MSE: {mse:.4f}, R²: {r2:.4f}")

async def main():

    await State.init_beanie()

    model, x_scaler, y_scaler = await train()

    await pred(model, x_scaler, y_scaler)

asyncio.run(main())


    # window_size = 4
    # await TopMoverResultDoc.aggregate([
    #     group_by_created_at,
    #     sort_by_id_stage,
    #     # set_number_stage,
    #     # # {"$skip": window_size},
    #     # # {"$unwind": "$items"},
    #     # # {"$replaceRoot": {"newRoot": "$items"} },
    #     {'$out': 'grouped_movers'}
    # ]).to_list()
    # grouped_movers = State.client['yaatdb']['grouped_movers']
    # # pprint(await shifted_movers.to_list())

    # # print(await grouped_movers.find().skip(window_size-1).limit(1).to_list())
    # async for movers in grouped_movers.find().skip(window_size-1):
    #     times = [movers['_id']]
    #     cushion = td(minutes=2)
    #     # subtract 15 and go back 3
    #     for _ in range(3):
    #         times.append(times[-1] - td(minutes=15))
    #         conds = [
    #             {'created_at': {'$gte': times[-1] - cushion}},
    #             {'created_at': {'$lte': times[-1] + cushion}}
    #         ]


    #     idx = movers['idx'] # 4
    #     # window_size lte gte pairs
        
    #     for mover in window['items']:
    #         # get last 4 groups, get docs
    #         previous_movers = await grouped_movers.aggregate([
    #             {'$match': {'idx' : {'$gt': idx-4}}},
    #             {'$match': {'idx' : {'$lt': idx}}},
    #             {"$unwind": "$items"},
    #             {"$replaceRoot": {"newRoot": "$items"}},
    #             {"$match": {'cid': mover['cid']}},
    #             group_by_created_at,
    #             sort_by_id_stage,
    #         ]).to_list()

    #         print("======= previous movers for ", mover['cid'], " =========== ")
    #         pprint(previous_movers)


    # #     pass
    # # grouped_movers = State.client['yaatdb']['grouped_movers']

    # # pipeline = [{"$skip": window_size}, {"$unwind": "$items"}, {"$replaceRoot": {"newRoot": "$items"} }]
    # # async for doc in TopMoverResultDoc.aggregate(pipeline):


    # # to=dt.now()
    # # _from = to - td(hours=1)
    # # (await CoinGecko.historical_chart_range('loom-network', vs_currency='usd',  **{'from':_from.timestamp()},
    # #                                               to=to.timestamp(), precision='10'))
    # # print ('-'*50)
    # # (await CoinGecko.historical_chart_range('loom-network', vs_currency='usd',  **{'from':_from.timestamp()},
    # #                                               to=to.timestamp(), precision='10'))
    # # # g_data = await CoinGecko.historical_chart_range(cid, vs_currency='usd', **{'from':date.timestamp()}, to=(date + td(minutes=5)).timestamp(), precision='10')
    # # # cg_data = await CoinGecko.historical_chart_range(cid, vs_currency='usd', **{'from':midpoint.timestamp()}, to=(midpoint + td(minutes=5)).timestamp(), precision='10')
    
    # # pass
