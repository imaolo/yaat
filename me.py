from mongoengine import connect, Document, DateTimeField, StringField, FloatField, IntField, StringField, ReferenceField
from pymongo import UpdateOne
from abc import ABC, ABCMeta, abstractmethod
from datetime import datetime, timezone
from typing import ClassVar, TypeVar, Generic, override, Any
import math

#### Abstract

class AbstractDocumentMeta(Document.my_metaclass, ABCMeta):
    pass

### Abstract Abstract

class UpdateAtomicDoc(Document, ABC, metaclass=AbstractDocumentMeta):
    meta = {'abstract': True}

    @property
    @abstractmethod
    def update_op(self) -> UpdateOne:
        pass

    @override
    def save(self):
        return type(self)._get_collection().bulk_write([self.update_op])

    @classmethod
    def insert(cls, docs: list['UpdateAtomicDoc']):
        return cls._get_collection().bulk_write([doc.update_op for doc in docs])

class AtomicDoc(UpdateAtomicDoc, ABC, metaclass=AbstractDocumentMeta):
    meta = {'abstract': True}

    def __init_subclass__(cls):
        super().__init_subclass__()

        if cls._meta['indexes']:
            raise TypeError("cannot have preexisting indexes")

        cls.required_fields = {name: field for name, field in cls._fields.items() if field.required}

        if all(choices:=[field.choices for field in cls.required_fields.values()]):
            cls._meta.update({'max_documents': math.prod(len(choice) for choice in choices)})
        cls._meta.update({'indexes': [{'fields': cls.required_fields.keys(), 'unique': True}] })

    @property
    def update_op(self) -> UpdateOne:
        query = {name: getattr(self, name) for name, _ in self.required_fields.items()}
        update = {"$setOnInsert": query}
        return UpdateOne(query, update, upsert=True)

ParamT = TypeVar('ParamT')
class IntervalJobDoc(UpdateAtomicDoc, ABC, Generic[ParamT], metaclass=AbstractDocumentMeta):
    meta = {
        'abstract': True,
        'indexes': [{'fields': ['params'], 'unique': True}]
    }

    interval_seconds = IntField(required=True)

    @property
    @abstractmethod
    def params(self) -> ParamT: pass

    @abstractmethod
    def interval_job(self, ctx: Any | None = None): pass

    @property
    def update_op(self) -> UpdateOne:
        query = {'params': self.params.pk}
        update = {"$set": {"interval_seconds": self.interval_seconds}}
        return UpdateOne(query, update, upsert=True)

#### Top Mover Docs

class TopMoverQueryDoc(AtomicDoc):
    durations: ClassVar[list[str]] = ['1h', '24h', '7d', '14d', '30d', '1y']
    top_coins: ClassVar[list[str]] = ['300', '500', '1000', 'all']

    duration = StringField(required=True, choices=durations)
    top_coin = StringField(required=True, choices=top_coins)

class TopMoverDoc(Document):
    created_at = DateTimeField(default_factory=datetime.now(timezone.utc))
    query = ReferenceField(TopMoverQueryDoc, required=True)
    id = StringField(required=True)
    symbol = StringField(required=True)
    name = StringField(required=True)
    usd = FloatField(required=True)
    market_cap_rank = IntField(required=True)
    usd_24h_vol = IntField(required=True)
    usd_1y_change = IntField(required=True)


def call_cg(*args, **kwargs) -> dict:
    pass

class TopMoverJobDoc(IntervalJobDoc[TopMoverQueryDoc]):
    params = ReferenceField(TopMoverQueryDoc)

    def interval_job(self, cg: Any):
        TopMoverDoc(**(cg(self.params.duration, self.params.top_coin) | {**self.query})).save()

connect('mydb')

TopMoverQueryDoc.drop_collection()
TopMoverJobDoc.drop_collection()

# create queries

TopMoverQueryDoc.insert([
    TopMoverQueryDoc(duration=duration, top_coin=top_coin)
        for duration in TopMoverQueryDoc.durations
        for top_coin in TopMoverQueryDoc.top_coins
])

# create jobs

TopMoverJobDoc.insert([TopMoverJobDoc(interval_seconds=10, params=query) for query in TopMoverQueryDoc.objects()])

# start jobs




# TopMoverJobDoc._get_collection().bulk_write()

# # TopMoverQueryDoc.drop_collection()
# # TopMoverJobDoc.objects.insert(jobs)
# print(type(TopMoverJobDoc.objects))



