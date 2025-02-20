from __future__ import annotations
from bson import Int64, ObjectId
from datetime import datetime
from dataclasses import dataclass, fields, asdict
from typing import Union, get_origin, get_args, TYPE_CHECKING, Type
from abc import ABC, abstractmethod
from typing import get_origin, get_args, TypeAlias, final, Any, Callable
import types, copy
if TYPE_CHECKING:
    from pymongo.synchronous.database import Collection, Database
    from pymongo import MongoClient
    from collections.abc import Iterable


class _PyBSON_TMap:
    def __init__(self):
        self.pybson_prim_map = {
            # TODO np.array
            str: 'string',
            int: 'int',
            bool: 'bool',
            float: 'double',
            datetime: 'date',
            ObjectId: 'objectId',
            Int64: 'long',
            float: 'double',
            dict: 'object',
        }

        self.pybson_tmap = {
            lambda T: T in self.pybson_prim_map: lambda T: {'bsonType': self.pybson_prim_map[T]},
            lambda T: not isinstance(T, types.GenericAlias) and issubclass(T, MongoDoc): lambda T: self.get_doc(T),
            lambda T: type is list or get_origin(T) is list: lambda T: self.get_list(T),
            lambda T: get_origin(T) is Union and type(None) in get_args(T): lambda T: self.get_optional(T),
        }

    def get_doc(self, doct: type[MongoDoc]) -> dict:
        return {'bsonType': 'object',
                'properties': {field.name: self[field.type] for field in fields(doct)},
                'required':  [field.name for field in fields(doct)],
                'additionalProperties': False}

    def get_list(self, listt: Iterable) -> dict:
        elemt, = get_args(listt)
        return {'bsonType': 'array', 'items': self[elemt]}

    def get_optional(self, optt: Any) -> dict:
        t, nt = get_args(optt)
        if nt is not type(None): raise RuntimeError(nt)
        (d:=self[t]).update({'bsonType': ['null', d['bsonType']]})
        return d

    def __getitem__(self, T: Any) -> dict:
        schema, = [schema(T) for check, schema in self.pybson_tmap.items() if check(T)]
        return schema

PyBSON_TMap = _PyBSON_TMap()

BSON_OBJ_TYPE: TypeAlias = dict[str, 'BSON_TYPE']
BSON_LIS_TYPE: TypeAlias = list['BSON_TYPE']
BSON_TYPE: TypeAlias = BSON_OBJ_TYPE | BSON_LIS_TYPE | str

@dataclass(frozen=True, kw_only=True)
class MongoDoc(ABC):

    def asdict(self) -> dict: return asdict(self)

    # TODO $jsonschema
    @classmethod
    def get_schema(cls) -> dict:
        (schema:=PyBSON_TMap[cls])['properties'].update({'_id': {'bsonType': 'objectId'}})
        return schema

    @classmethod
    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        dataclass(cls, kw_only=True, frozen=True)

class MongoCommitDoc(MongoDoc, ABC):
    pass
#     ''' Doc has a database init procedure '''
#     @abstractmethod
#     def init(cls, *args, **kwargs):
#         pass

# @final
class MongoCollectionDoc(MongoCommitDoc):
    pass
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
            self.coll.database.command('collMod', self.coll.name, validator={'$jsonSchema':self.doctype.get_schema()})
        else:
            self.coll.database.create_collection(self.coll.name, validator={'$jsonSchema':self.doctype.get_schema()})