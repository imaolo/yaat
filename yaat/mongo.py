from __future__ import annotations
from bson import Int64, ObjectId
from datetime import datetime
from dataclasses import dataclass, fields, asdict
from typing import Union, get_origin, get_args, TYPE_CHECKING, Type
from abc import ABC, abstractmethod
from typing import get_origin, get_args, TypeAlias, final, Any, Callable, Union, get_type_hints
import types, copy
if TYPE_CHECKING:
    from pymongo.synchronous.database import Collection, Database
    from pymongo import MongoClient
    from collections.abc import Iterable

PyBSONPrimType = (
    type[str] |
    type[int] |
    type[bool] |
    type[float] |
    type[datetime] |
    type[ObjectId] |
    type[Int64] |
    type[dict]
)

pybson_prim_map: dict[PyBSONPrimType, str] = {
    # TODO np.array
    str: 'string',
    int: 'int',
    bool: 'bool',
    float: 'double',
    datetime: 'date',
    ObjectId: 'objectId',
    Int64: 'long',
    dict: 'object',
}

SCHEMA_DICT_VALUE_T = str | list[str] | dict[str, 'SCHEMA_DICT_VALUE_T']
SCHEMA_DICT_T = dict[str, SCHEMA_DICT_VALUE_T]

DOC_DICT_VALUE_T = PyBSONPrimType | list[PyBSONPrimType] | dict[str, 'DOC_DICT_VALUE_T']
DOC_DICT_T = dict[str, DOC_DICT_VALUE_T]

Mongo_Docs: dict[str, type[MongoDoc]] = {}
@dataclass(frozen=True, kw_only=True)
class MongoDoc(ABC):

    @classmethod
    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        dataclass(cls, kw_only=True, frozen=True)
        type_hints = get_type_hints(cls)
        for field in fields(cls):
            th = type_hints[field.name]
            if cls.is_primitive(th): continue
            if cls.is_mongodoc(th): continue
            if cls.is_list(th): continue
            if cls.is_optional(th): continue
            if cls.is_mongodoc_type(th): continue
            raise RuntimeError(f"invalid MongoDoc class {cls}, {field}, {th}")
        Mongo_Docs[cls.__name__] = cls

    @classmethod
    def get_schema(cls) -> SCHEMA_DICT_T:
        (schema:=PyBSON_TMap[cls])['properties'].update({'_id': {'bsonType': 'objectId'}})
        return {'$jsonSchema': schema}

    @property
    def dict(self) -> DOC_DICT_T: return asdict(self)

    @staticmethod
    def is_primitive(t: PyBSONPrimType | Any) -> bool: return t in pybson_prim_map

    @staticmethod
    def is_mongodoc(t: type[MongoDoc] | Any) -> bool: return get_origin(t) is None and issubclass(t, MongoDoc)

    @staticmethod
    def is_list(t: type[list[PyBSONType]] | Any) -> bool: return type is list or get_origin(t) is list

    @staticmethod
    def is_optional(t: type[Union[PyBSONType, None]] | Any) -> bool: return get_origin(t) is Union and type(None) in get_args(t)

    @staticmethod
    def is_mongodoc_type(t: Type[Type[MongoDoc]] | Any) -> bool: return get_origin(t) is type and issubclass(get_args(t)[0], MongoDoc)

PyBSONType = PyBSONPrimType | type[MongoDoc] | type[list] | type[Union['PyBSONType', None]]
class _PyBSON_TMap:
    def __init__(self):
        self.pybson_tmap: dict[Callable[[type[Any]], bool], Callable[[type[Any]], SCHEMA_DICT_T]] = {
            MongoDoc.is_primitive: self.get_primitive,
            MongoDoc.is_mongodoc: self.get_mongodoc,
            MongoDoc.is_list: self.get_list,
            MongoDoc.is_optional: self.get_optional,
            MongoDoc.is_mongodoc_type: self.get_mongodoc_type
        }

    def __getitem__(self, T: type[Any]) -> SCHEMA_DICT_T:
        schema, = [schema(T) for check, schema in self.pybson_tmap.items() if check(T)]
        return schema

    @staticmethod
    def get_primitive(t: PyBSONPrimType) -> SCHEMA_DICT_T: return {'bsonType': pybson_prim_map[t]}

    def get_mongodoc(self, t: type[MongoDoc]) -> SCHEMA_DICT_T:
        return {'bsonType': 'object',
                'properties': {field.name: self[field.type] for field in fields(t)},
                'required':  [field.name for field in fields(t)],
                'additionalProperties': False}

    def get_list(self, t: type[list[PyBSONType]]) -> SCHEMA_DICT_T:
        t, = get_args(t)
        return {'bsonType': 'array', 'items': self[t]}

    def get_optional(self, t: type[Union[PyBSONType, None]]) -> SCHEMA_DICT_T:
        t, nt = get_args(t)
        if nt is not type(None): raise RuntimeError(nt)
        (d:=self[t]).update({'bsonType': ['null', d['bsonType']]})
        return d

    def get_mongodoc_type(self, _: Type[Type[MongoDoc]]): return {'bsonType': 'str'}

PyBSON_TMap = _PyBSON_TMap()

class MongoCommitDoc(MongoDoc, ABC):
    pass
#     ''' Doc has a database init procedure '''
#     @abstractmethod
#     def init(cls, *args, **kwargs):
#         pass

# # @final
# class MongoCollectionDoc(MongoDoc):
#     name: int
#     doc: type[MongoDoc]

#     name: str
#     schema: type[MongoDoc]
#     name: str
#     schema: dict
#     # TODO - index, time series information, etc, others creation info

#     def init(self, db: Database):
#         coll = db[self.name]
#         if coll.name in coll.database.list_collection_names():
#             coll.database.command('collMod', coll.name, validator={'$jsonSchema':self.doc.get_schema()})
#         else:
#             coll.database.create_collection(coll.name, validator={'$jsonSchema':self.doc.get_schema()})
#         pass

# @final
class MongoDatabaseDoc(MongoCommitDoc):
    pass
#     name: str
#     colls: list[MongoCollectionDoc]
#     # TODO other information about the db, security, collections, etc 

#     def commit(self, dbc: MongoClient):
#         db = dbc[self.name]
#         for coll in self.colls: coll.commit(db)


class MongoCollection:
    def __init__(self, coll:Collection, doctype: Type[MongoDoc]):
        self.coll = coll
        self.doctype = doctype

        if self.coll.name in self.coll.database.list_collection_names():
            self.coll.database.command('collMod', self.coll.name, validator=self.doctype.get_schema())
        else:
            self.coll.database.create_collection(self.coll.name, validator=self.doctype.get_schema())