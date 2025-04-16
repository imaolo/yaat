from __future__ import annotations
from yaat.doc import Doc, DocArgs
from pydantic import BaseModel, Field, field_serializer
from enum import Enum
from datetime import datetime, timezone
from yaat.job import IntervalJobDoc
import random

class TopMoverQueryDoc(BaseModel):
    class Duration(str, Enum):
        h1 = "1h"
        d24 = "24h"
        d7 = "7d"
        d14 = "14d"
        d30 = "30d"
        y1 = "1y"

    class TopCoins(str, Enum):
        coins_300 = "300"
        coins_500 = "500"
        coins_1000 = "1000"
        coins_all = "all"

    duration: Duration = Field(..., description="duration of top movers")
    top_coin: TopCoins = Field(..., description="how many ordered coins to fetch")

class TopMoverResultDoc(Doc, doc_args=DocArgs(schema_updateable=False, db_updateable=False)):
    # https://docs.coingecko.com/reference/coins-top-gainers-losers
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    query: TopMoverQueryDoc
    cid: str
    symbol: str
    name: str
    usd: float
    market_cap_rank: int
    usd_24h_vol: int
    usd_1y_change: int
    # TODO add a % change column

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
        await top_mover_func(self.query)

async def top_mover_func(query: TopMoverQueryDoc):
    url = "https://pro-api.coingecko.com/api/v3"
    # https://docs.coingecko.com/reference/coins-top-gainers-losers
    # TODO get the % change
    await TopMoverResultDoc.insert_many([TopMoverResultDoc(
        query=query,
        cid='fdsafdsa',
        symbol='btc',
        name='bitcoin',
        usd=random.uniform(1000.0, 1000000.0),
        market_cap_rank=mcr,
        usd_24h_vol=random.randint(100, 100000),
        usd_1y_change=random.randint(-2**31, 2**31)
    ) for mcr in range(1, int(query.top_coin.value if query.top_coin.value != 'all' else 2000)+1)])