from __future__ import annotations
from yaat.state import APSJobDoc
from yaat.doc import ReadOnlyDoc, Doc
from datetime import datetime, timezone
from beanie import Link, Document
from typing import Optional, TypeVar, Generic
from pydantic import Field
from enum import Enum
from abc import ABC, abstractmethod
from typing import ClassVar
from beanie import Document
from beanie.operators import Set
from itertools import product

# Abstract classes

ResultT = TypeVar('ResultT', bound=Document)
QueryT = TypeVar('QueryT', bound=Document)

class Query(Generic[ResultT], ABC):
    # TODO - override insert, prevent update, delete
    # TODO - unique indexes
    @abstractmethod
    def __call__(self) -> list[ResultT]: pass

class Result(Generic[QueryT], ABC):
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    query: Link[QueryT]

    @classmethod
    def __class_getitem__(cls, queryt) -> type:
        # TODO still needed?
        new_cls = super().__class_getitem__(queryt)
        new_cls.queryt = queryt
        return new_cls

class JobDoc(Doc, ABC):
    apsjob: Optional[Link[APSJobDoc]] = None

#### Top Mover Query and Result Docs
# https://docs.coingecko.com/reference/coins-top-gainers-losers

class TopMoverQueryDoc(ReadOnlyDoc, Query['TopMoverResultDoc']):
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

    async def insert(self, *args, **kwargs):
        return await type(self).find_one(doc:=self.model_dump(exclude=['id'])).upsert(Set(doc), on_insert=self)

    @classmethod
    async def init(cls):
        for duration, top_coin in cls.durations_top_coins:
            q = cls(duration=duration.value, top_coin=top_coin.value).model_dump()
            match await cls.find(q, limit=2).count():
                case 0: await cls.get_motor_collection().insert_one(q) # exception to rule
                case 1: pass
                case _: raise RuntimeError()

    def __call__(self) -> TopMoverResultDoc:
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

class TopMoverResultDoc(ReadOnlyDoc, Query['TopMoverQueryDoc']):
    # https://docs.coingecko.com/reference/coins-top-gainers-losers
    cid: str
    symbol: str
    name: str
    usd: float
    market_cap_rank: int
    usd_24h_vol: int
    usd_1y_change: int

    @classmethod
    def model_json_schema(cls):
        # TODO hack
        q = TopMoverQueryDoc.model_json_schema()
        t = super().model_json_schema()
        if '$defs' not in t: t['$defs'] = {}
        q['properties'].pop('_id')
        t['$defs'].update(q.pop('$defs') | {q['title']: q})
        t['properties']['query'] = {'$ref': '#/$defs/'+q['title']}
        return t

class TopMoverJobDoc(JobDoc):
    seconds: int
    query: Link[TopMoverQueryDoc]

    # TODO add after hooks for interacting with actual scheduler
    # TODO - crud jobs