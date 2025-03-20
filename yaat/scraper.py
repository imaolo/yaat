from __future__ import annotations
from yaat.state import State, APSJobDoc, JobDoc
from yaat.doc import ReadOnlyDoc, Doc
from beanie import Link
from beanie.operators import Set
from beanie.odm.actions import before_event, Insert
from pydantic import Field
from enum import Enum
from typing import ClassVar
from itertools import product
from datetime import datetime, timezone

#### Top Mover Query and Result Docs
# https://docs.coingecko.com/reference/coins-top-gainers-losers

class TopMoverQueryDoc(ReadOnlyDoc):
    readable: ClassVar[bool] = False

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

    durations_top_coins: ClassVar[tuple[Duration, TopCoins]] = list(product(Duration, TopCoins))

    duration: Duration = Field(..., description="duration of top movers")
    top_coin: TopCoins = Field(..., description="how many ordered coins to fetch")

    async def insert(self, *args, **kwargs) -> Doc:
        await type(self).find_one(doc:=self.model_dump(exclude=['id'])).upsert(Set(doc), on_insert=self)
        return await type(self).find_one(doc)

    @classmethod
    async def init(cls):
        for duration, top_coin in cls.durations_top_coins:
            q = cls(duration=duration.value, top_coin=top_coin.value).model_dump()
            match await cls.find(q, limit=2).count():
                case 0: await cls.get_motor_collection().insert_one(q) # exception to rule
                case 1: pass
                case _: raise RuntimeError()

    def __call__(self) -> list[TopMoverResultDoc]:
        return [TopMoverResultDoc(
            query=self,
            cid='fdsafdsa',
            symbol='btc',
            name='bitcoin',
            usd=100.0,
            market_cap_rank=1,
            usd_24h_vol=1,
            usd_1y_change=1
        )]

class TopMoverResultDoc(ReadOnlyDoc):
    # https://docs.coingecko.com/reference/coins-top-gainers-losers
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    query: Link[TopMoverQueryDoc]
    cid: str
    symbol: str
    name: str
    usd: float
    market_cap_rank: int
    usd_24h_vol: int
    usd_1y_change: int

class TopMoverJobDoc(JobDoc):
    seconds: int
    query: Link[TopMoverQueryDoc]

    @before_event(Insert)
    async def before_create_trg(self):
        await super().before_create_trg()
        job = State.scheduler.add_job(self.job, 'interval', seconds=self.seconds)
        self.apsjob = await APSJobDoc.get(job.id)

    @classmethod
    async def crud_d(cls, id: str) -> TopMoverJobDoc:
        doc: TopMoverJobDoc = await super().crud_d(id)
        await doc.fetch_all_links()
        State.scheduler.remove_job(doc.apsjob.id)
        return doc

    async def job(self):
        await self.fetch_all_links()
        await TopMoverResultDoc.insert_many(self.query())

    @classmethod
    async def crud_c(cls, doc: dict):
        query = await TopMoverQueryDoc(**doc['query']).insert()
        return await cls(seconds=doc['seconds'], query=query.to_ref()).insert()
        