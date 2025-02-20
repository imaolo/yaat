from __future__ import annotations
from bson import Int64, ObjectId
from datetime import datetime
from dataclasses import dataclass, fields, asdict
from typing import Union, get_origin, get_args, TYPE_CHECKING, Type
from abc import ABC
from typing import get_origin, get_args, TypeAlias
import types
if TYPE_CHECKING:
    from pymongo.synchronous.database import Collection
    from collections.abc import Iterable

pybson_tmap = {
    # TODO np.array
    str: 'string',
    int: 'int',
    bool: 'bool',
    float: 'double',
    datetime: 'date',
    ObjectId: 'objectId',
    Int64: 'long',
    float:'double',
}

BSON_OBJ_TYPE: TypeAlias = dict[str, 'BSON_TYPE']
BSON_LIS_TYPE: TypeAlias = list['BSON_TYPE']
BSON_TYPE: TypeAlias = BSON_OBJ_TYPE | BSON_LIS_TYPE | str

@dataclass(frozen=True, kw_only=True)
class MongoDoc(ABC):

    def asdict(self) -> dict: return asdict(self)

    @classmethod
    def get_schema(cls) -> dict: return cls._get_schema(cls, include_id=True)

    @classmethod
    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        dataclass(cls, kw_only=True, frozen=True)

    @classmethod
    def _get_schema(cls, t: Union[MongoDoc, list, *pybson_tmap.keys()], include_id: bool = False) -> BSON_OBJ_TYPE: # type: ignore
        bson_type = []

        # HACK for Optionals, could strip the Union better
        if cls._is_optional_type(t): # NOTE could strip union better
            t, nt = get_args(t)
            if nt is not type(None): raise RuntimeError(nt)
            bson_type.append('null')

        if t in pybson_tmap:
            return {'bsonType': [pybson_tmap[t]] + bson_type}
        elif cls._is_doc_type(t):
            return {'bsonType': ['object'] + bson_type ,
                    'properties': t._get_properties() |  ({'_id': {'bsonType': 'objectId'}} if include_id else {}),
                    'required':  t._get_required(),
                    'additionalProperties': False}
        elif cls._is_list_type(t):
            return {'bsonType': ['array'] + bson_type,
                    'items': cls._get_items(t)}
        else:
            raise RuntimeError(t)

    @classmethod
    def _get_required(cls) -> list[str]: return [field.name for field in fields(cls)]

    @classmethod
    def _get_properties(cls) -> BSON_OBJ_TYPE:
        return {field.name: cls._get_schema(field.type) for field in fields(cls)}

    @classmethod
    def _get_items(cls, list_type: Iterable) -> BSON_OBJ_TYPE:
        elem_type, = get_args(list_type)
        return cls._get_schema(elem_type)

    @staticmethod
    def _is_list_type(typ) -> bool:
        return type is list or get_origin(typ) is list

    @staticmethod
    def _is_doc_type(t) -> bool:
        return not isinstance(t, types.GenericAlias) and issubclass(t, MongoDoc)

    @staticmethod
    def _is_optional_type(t) -> bool:
        return get_origin(t) is Union and type(None) in get_args(t)

class MongoCollection:
    # TODO - index information

    def __init__(self, coll:Collection, doctype: Type[MongoDoc]):
        self.coll = coll
        self.doctype = doctype

        if self.coll.name in self.coll.database.list_collection_names():
            self.coll.database.command('collMod', self.coll.name, validator={'$jsonSchema':self.doctype.get_schema()})
        else:
            self.coll.database.create_collection(self.coll.name, validator={'$jsonSchema':self.doctype.get_schema()})