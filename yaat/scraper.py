from mongoengine import Document, DateTimeField, StringField, FloatField, IntField, StringField, ReferenceField
from pymongo import UpdateOne
from datetime import datetime, timezone
from typing import ClassVar
from yaat.db import UpdateCacheDoc, InsertOnlyCacheDoc
from marshmallow_mongoengine import ModelSchema, fields

#### Top Mover Docs

class TopMoverQueryDoc(InsertOnlyCacheDoc):
    durations: ClassVar[list[str]] = ['1h', '24h', '7d', '14d', '30d', '1y']
    top_coins: ClassVar[list[str]] = ['300', '500', '1000', 'all']

    duration = StringField(required=True, choices=durations)
    top_coin = StringField(required=True, choices=top_coins)

class TopMoverQueryDocSchema(ModelSchema):
    class Meta:
        model = TopMoverQueryDoc
        exclude = ("id",)

class TopMoverDoc(Document):
    # https://docs.coingecko.com/reference/coins-top-gainers-losers
    created_at = DateTimeField(default_factory=lambda: datetime.now(timezone.utc))
    query = ReferenceField(TopMoverQueryDoc, required=True)
    cid = StringField(required=True)
    symbol = StringField(required=True)
    name = StringField(required=True)
    usd = FloatField(required=True)
    market_cap_rank = IntField(required=True)
    usd_24h_vol = IntField(required=True)
    usd_1y_change = IntField(required=True)

class TopMoverDocSchema(ModelSchema):
    query = fields.Nested(TopMoverQueryDocSchema) 
    class Meta:
        model = TopMoverDoc
        exclude = ("id",)

class TopMoverJobDoc(UpdateCacheDoc):
    meta = {'indexes': [{'fields': ['params'], 'unique': True}]}

    interval_seconds = IntField(required=True)
    params = ReferenceField(TopMoverQueryDoc)

    @property
    def update_op(self) -> UpdateOne:
        query = {'params': self.params.pk}
        update = {"$set": {"interval_seconds": self.interval_seconds}}
        return UpdateOne(query, update, upsert=True)

    def interval_job(self):
        TopMoverDoc(
            query = self.params,
            cid = 'fdsafdsa',
            symbol = 'btc',
            name = 'bitcoin',
            usd = 100,
            market_cap_rank = 100,
            usd_24h_vol = 100,
            usd_1y_change = 100).save()

#### Start

def run():
    from apscheduler.schedulers.background import BackgroundScheduler

    # create queries
    TopMoverQueryDoc.bulk_save([
        TopMoverQueryDoc(duration=duration, top_coin=top_coin)
            for duration in TopMoverQueryDoc.durations
            for top_coin in TopMoverQueryDoc.top_coins
    ])

    # create jobs
    TopMoverJobDoc.bulk_save([TopMoverJobDoc(interval_seconds=10*10, params=query) for query in TopMoverQueryDoc.objects()])

    # start jobs
    schedule = BackgroundScheduler()
    for job in TopMoverJobDoc.objects():
        schedule.add_job(job.interval_job, 'interval', seconds=job.interval_seconds)
    schedule.start()
