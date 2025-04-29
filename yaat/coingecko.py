from __future__ import annotations
from yaat.doc import Doc, DocArgs
from yaat.helpers import fetchjson
from pydantic import BaseModel, Field, field_serializer, WithJsonSchema
from enum import Enum
from datetime import datetime, timezone
from yaat.job import IntervalJobDoc
from datetime import datetime, timedelta as td
from typing import Annotated

class CoinGeckoCacheDoc(Doc, doc_args=DocArgs(schema_readable=False)):
    hash: str
    result: list | dict

class CoinGecko:
    url: str = 'https://pro-api.coingecko.com/api/v3/'
    key : str = 'CG-ivjvmcDsabTGQg25HpTUa7H5'

    @classmethod
    async def fetch(cls, ext, **kwargs):
        return await fetchjson(cls.url + ext,
                               headers={"accept": "application/json", "x-cg-pro-api-key": cls.key},
                               **kwargs)

    @classmethod
    async def top_gainers_losers(cls, **kwargs) -> dict:
        return await cls.fetch('coins/top_gainers_losers', **kwargs)

    @classmethod
    async def coin_data(cls, cid:str, **kwargs) -> dict:
        return await cls.fetch(f"coins/{cid}", **kwargs)

    @classmethod
    async def historical_chart_range(cls, cid, **kwargs) -> dict:
        result = await CoinGeckoCacheDoc.get_motor_collection().find_one({'_id' : (_id := str(kwargs)+'historical_chart_range'+cid)})
        if not result:
            result = await cls.fetch(f"coins/{cid}/market_chart/range/", **kwargs)
            try:
                await CoinGeckoCacheDoc.get_motor_collection().insert_one({'_id': _id} | result)
            except Exception as e:
                print("historical_chart_range exception! trying again")
                print(e)
                result = await CoinGeckoCacheDoc.get_motor_collection().find_one({'_id' : (_id := str(kwargs)+'historical_chart_range'+cid)})
                if not result:
                    raise RuntimeError()
        return result

class TopMoverQueryDoc(BaseModel):
    class Duration(str, Enum):
        h1 = "1h"
        h24 = "24h"
        d7 = "7d"
        d14 = "14d"
        d30 = "30d"
        y1 = "1y"

    class TopCoins(str, Enum):
        coins_300 = "300"
        coins_500 = "500"
        coins_1000 = "1000"
        coins_all = "all"

    vs_currency: str = 'usd'
    duration: Duration = Field(..., description="duration of top movers")
    top_coin: TopCoins = Field(..., description="how many ordered coins to fetch")

    @field_serializer('duration')
    def duration_field_serializer(self, duration: Duration) -> str: return duration.value
    
    @field_serializer('top_coin')
    def top_coin_field_serializer(self, top_coin: TopCoins) -> str: return top_coin.value

    @property
    def timedelta(self) -> td:
        match self.duration:
            case self.Duration.h1: return td(hours=1)
            case self.Duration.h24: return td(hours=24)
            case self.Duration.d7: return td(days=7)
            case self.Duration.d14: return td(days=14)
            case self.Duration.d30: return td(days=30)
            case self.Duration.y1: return td(weeks=52)
            case _: raise RuntimeError(self.duration)

class TopMoverResultDoc(Doc, doc_args=DocArgs(schema_updateable=False, db_updateable=False)):
    # https://docs.coingecko.com/reference/coins-top-gainers-losers
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    query: TopMoverQueryDoc
    cid: str
    symbol: str
    name: str
    usd: float
    market_cap_rank: int
    usd_24h_vol: float
    percent_change: float
    hour_later_change: Annotated[float | None, WithJsonSchema({'type':'number'})] = None

    @field_serializer('created_at')
    def serialize_created_at(self, dt: datetime, _info) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")

    class Settings:
        indexes = [
            [("created_at", -1), ('query.duration', 1), ('query.top_coin', 1)]
        ]

class TopMoverJobDoc(IntervalJobDoc, doc_args=DocArgs()):
    query: TopMoverQueryDoc

    @classmethod
    async def crud_c(cls, doc: dict) -> None:
        await cls(query=TopMoverQueryDoc(**doc.pop('query')), **doc).create()

    async def func(self):
        # https://docs.coingecko.com/reference/coins-top-gainers-losers
        cg_data: list[dict] = (await CoinGecko.top_gainers_losers(**self.query.model_dump()))
        created_at = datetime.now(timezone.utc)
        top_movers: list = cg_data['top_gainers']
        top_movers.extend(cg_data['top_losers'])
        result_docs: list = []
        date = datetime.now() - self.query.timedelta
        for tm in top_movers:
            tm['cid'] = tm.pop('id')
            tm.pop('image')
            async def get_percent_change(cid: str) -> float:
                cg_data = await CoinGecko.historical_chart_range(cid, vs_currency='usd', **{'from':date.timestamp()}, to=(date + td(minutes=5)).timestamp(), precision='10')
                midpoint = date
                increments = (datetime.now() - midpoint)/8
                while not cg_data['prices'] and midpoint < datetime.now() - td(minutes=4):
                    print(f"searching for valid date ({midpoint}) for {tm['cid']}")
                    midpoint = midpoint + increments
                    cg_data = await CoinGecko.historical_chart_range(cid, vs_currency='usd', **{'from':midpoint.timestamp()}, to=(midpoint + td(minutes=5)).timestamp(), precision='10')
                try: _, old_price = cg_data['prices'][0]
                except:
                    from pprint import pprint
                    pprint(cg_data)
                    pprint(tm)
                    raise
                cg_data = await CoinGecko.coin_data(cid, tickers='false', community_data='false', developer_data='false', market_data='true')
                current_price = cg_data['market_data']['current_price']['usd']
                return ((current_price-old_price)/old_price)*100
            tm['percent_change'] = await get_percent_change(tm['cid'])
            result_docs.append(TopMoverResultDoc(query=self.query, created_at=created_at, **tm))
        await TopMoverResultDoc.insert_many(result_docs)
