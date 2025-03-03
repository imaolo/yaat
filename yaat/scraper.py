from datetime import datetime, timezone
from beanie import Link, Document
from pydantic import Field
from enum import Enum

#### Top Mover Docs

class TopMoverQueryDoc(Document):
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
    
    # class Settings:
    #     name = "top_mover_docs"  # MongoDB collection name
    #     indexes = [
    #         "cid",  # Single-field index
    #         [("usd", -1)]  # Compound index example: descending order on usd
    #     ]

class TopMoverDoc(Document):
    # https://docs.coingecko.com/reference/coins-top-gainers-losers
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    query: Link[TopMoverQueryDoc] = Field(...)
    cid: str = Field(...)
    symbol: str = Field(...)
    name: str = Field(...)
    usd: float = Field(...)
    market_cap_rank: int = Field(...)
    usd_24h_vol: int = Field(...)
    usd_1y_change: int = Field(...)

    @classmethod
    def model_json_schema(cls):
        q = TopMoverQueryDoc.model_json_schema()
        t = super().model_json_schema()
        if '$defs' not in t: t['$defs'] = {}
        q['properties'].pop('_id')
        t['$defs'].update(q.pop('$defs') | {q['title']: q})
        t['properties']['query'] = {'$ref': '#/$defs/'+q['title']}
        import pprint
        pprint.pprint(t)
        return t

class TopMoverJobDoc(Document):
    seconds: int = Field(...)
    jid: int = Field(..., description='job id')
    query: Link[TopMoverQueryDoc] = Field(...)

class TopMoverJobDoc(Document):
    seconds: int = Field(...)
    jid: int = Field(..., description='job id')
    query: Link[TopMoverQueryDoc] = Field(...)



    # @property
    # def update_op(self) -> UpdateOne:
    #     query = {'params': self.params.pk}
    #     update = {"$set": {"interval_seconds": self.interval_seconds}}
    #     return UpdateOne(query, update, upsert=True)
