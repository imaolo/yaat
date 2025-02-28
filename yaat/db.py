from mongoengine import Document
from pymongo import UpdateOne
from abc import ABC, ABCMeta, abstractmethod
from typing import override
import math

class AbstractDocMeta(Document.my_metaclass, ABCMeta):
    pass

class UpdateCacheDoc(Document, ABC, metaclass=AbstractDocMeta):
    meta = {'abstract': True}

    @property
    @abstractmethod
    def update_op(self) -> UpdateOne:
        pass

    @override
    def save(self):
        if hasattr(self, 'id') and self.id: return self
        self.id, = type(self)._get_collection().bulk_write([self.update_op]).upserted_ids.values()
        return self

    @classmethod
    def bulk_save(cls, docs: list['UpdateCacheDoc']):
        update_docs = [doc for doc in docs if not(hasattr(doc, 'id') and doc.id)]
        ids = cls._get_collection().bulk_write([doc.update_op for doc in update_docs]).upserted_ids.values()
        for doc, id in zip(update_docs, ids):
            doc.id = id

class InsertOnlyCacheDoc(UpdateCacheDoc, ABC, metaclass=AbstractDocMeta):
    meta = {'abstract': True}

    def __init_subclass__(cls):
        super().__init_subclass__()

        if cls._meta['indexes']:
            raise TypeError("cannot have sub class indexes")

        cls.required_fields = {name: field for name, field in cls._fields.items() if field.required}

        if all(choices:=[field.choices for field in cls.required_fields.values()]):
            cls._meta.update({'max_documents': math.prod(len(choice) for choice in choices)})
        cls._meta.update({'indexes': [{'fields': cls.required_fields.keys(), 'unique': True}] })

    @property
    def update_op(self) -> UpdateOne:
        query = {name: getattr(self, name) for name, _ in self.required_fields.items()}
        update = {"$setOnInsert": query}
        return UpdateOne(query, update, upsert=True)

    @override
    def delete(self, *args, **kwargs):
        raise RuntimeError("insert only collection")
