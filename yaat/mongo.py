from __future__ import annotations
from bson import Int64, ObjectId
from datetime import datetime
from dataclasses import dataclass, fields, asdict
from typing import get_origin, get_args, Callable, Union, ClassVar, Any, final
from abc import ABC
from pymongo.synchronous.database import Collection, Database
from pymongo import MongoClient
import sys, inspect

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

PyBSONPrimType = Union[*pybson_prim_map.keys()]

SCHEMA_DICT_VALUE_T = str | list[str] | dict[str, 'SCHEMA_DICT_VALUE_T']
SCHEMA_DICT_T = dict[str, SCHEMA_DICT_VALUE_T]

DOC_DICT_VALUE_T = PyBSONPrimType | list[PyBSONPrimType] | dict[str, 'DOC_DICT_VALUE_T']
DOC_DICT_T = dict[str, DOC_DICT_VALUE_T]

MongoDoc_Names: dict[str, type['MongoDoc']] = {}
MongoDoc_Fields: dict[tuple[tuple[str, type]], type['MongoDoc']] = {}

@dataclass(frozen=True, kw_only=True)
class MongoDoc(ABC):
    schema: ClassVar[SCHEMA_DICT_T]
    collname: ClassVar[str]
    colldoc: ClassVar[CollDoc]
    fields: ClassVar[dict[str, Any]]

    @classmethod
    def __init_subclass__(cls, colldoc: CollDoc | None = None, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
    
        # each sub class is the same
        dataclass(cls, kw_only=True, frozen=True)

        # extract the collname
        cls.collname = cls.__name__
        cls.colldoc = colldoc

        # no repeating names
        if cls.collname in MongoDoc_Names:
            raise RuntimeError(f"repeating MongoDoc class names are disallowed {cls=} {MongoDoc_Names[cls.collname]=}")

        # set the type hints (skip class vars)
        g = sys.modules[cls.__module__].__dict__
        cls.fields = {
            field.name: eval(field.type, g) if isinstance(field.type, str) else field.type
            for field in fields(cls)
            if not(isinstance(field.type, str) and field.type.startswith('ClassVar'))
        }
        fieldstup = tuple(cls.fields.items()) 

        # no repeating fields
        if fieldstup in MongoDoc_Fields:
            raise RuntimeError(f"repeating MongoDoc class field sets are disallowed {cls=} {MongoDoc_Fields[fieldstup]=}")

        # set the schema
        (schema:=Py2BSON_Schema[cls])['properties'].update({'_id': {'bsonType': 'objectId'}})
        cls.schema = {'$jsonSchema': schema}

        # cache the class
        MongoDoc_Names[cls.collname] = MongoDoc_Fields[fieldstup] = cls

    @property
    def dict(self) -> DOC_DICT_T: return asdict(self)

PyBSONType = PyBSONPrimType | type[MongoDoc] | type[list] | type[Union['PyBSONType', None]]
class _Py2BSON_Schema:
    def __init__(self):
        self.pybson_tmap: dict[Callable[[type[Any]], bool], Callable[[type[Any]], SCHEMA_DICT_T]] = {
            self.is_primitive: self.get_primitive,
            self.is_mongodoc: self.get_mongodoc,
            self.is_list: self.get_list,
            self.is_optional: self.get_optional,
            self.is_mongodoc_type: self.get_mongodoc_type
        }

    def __getitem__(self, T: type[Any]) -> SCHEMA_DICT_T:
        schema, = [schema(T) for check, schema in self.pybson_tmap.items() if check(T)]
        return schema

    @staticmethod
    def is_primitive(t: PyBSONPrimType | Any) -> bool: return t in pybson_prim_map

    @staticmethod
    def is_mongodoc(t: type[MongoDoc] | Any) -> bool: return get_origin(t) is None and issubclass(t, MongoDoc)

    @staticmethod
    def is_list(t: type[list[PyBSONType]] | Any) -> bool: return t is list or get_origin(t) is list

    @staticmethod
    def is_optional(t: type[Union[PyBSONType, None]] | Any) -> bool: return get_origin(t) is Union and type(None) in get_args(t)

    @staticmethod
    def is_mongodoc_type(t: type[type[MongoDoc]] | Any) -> bool: return get_origin(t) is type and issubclass(get_args(t)[0], MongoDoc)

    @staticmethod
    def get_primitive(t: PyBSONPrimType) -> SCHEMA_DICT_T: return {'bsonType': pybson_prim_map[t]}

    def get_mongodoc(self, t: type[MongoDoc]) -> SCHEMA_DICT_T:
        return {'bsonType': 'object',
                'properties': {name: self[type] for name, type in t.fields.items()},
                'required':  list(t.fields.keys()),
                'additionalProperties': False}

    def get_list(self, t: type[list[PyBSONType]]) -> SCHEMA_DICT_T:
        ts = get_args(t)
        if not ts: return {'bsonType': 'array'}
        elif len(ts) == 1: return {'bsonType': 'array', 'items': self[ts[0]]}
        else: RuntimeError(f"only 0 or 1 list type argumented allowed - {t=}")

    def get_optional(self, t: type[Union[PyBSONType, None]]) -> SCHEMA_DICT_T:
        t, nt = get_args(t)
        if nt is not type(None): raise RuntimeError(nt)
        (d:=self[t]).update({'bsonType': ['null', d['bsonType']]})
        return d

    def get_mongodoc_type(self, _: type[type[MongoDoc]]): return {'bsonType': 'str'}

Py2BSON_Schema = _Py2BSON_Schema()

@final
class IndexDoc(MongoDoc):
    args: list
    kwargs: dict

    @classmethod
    def create(cls, *args, **kwargs) -> IndexDoc:
        return cls(args=args, kwargs=kwargs)

@final
class CollDoc(MongoDoc):
    index: IndexDoc


class MongoCollection(Collection, ABC):
    doc: ClassVar[type[MongoDoc]] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, *kwargs)

        if self.name in self.database.list_collection_names():
            if self.options().get('validator') != self.doc.schema:
                raise RuntimeError("illegal attempt to update schema")
        else:
            self.database.create_collection(self.name, validator=self.doc.schema)

        if self.doc.colldoc:
            self.create_index(self.doc.colldoc.index.args, **self.doc.colldoc.index.kwargs)

        # TODO time series information

    def __init_subclass__(cls, doc: type[MongoDoc]):
        cls.doc = doc

    def __class_getitem__(cls, k:Any) -> Any:
        if not Py2BSON_Schema.is_mongodoc(k): return None
        class MongoCollection(cls, doc=k):
            pass
        return MongoCollection

class ImplicitClassFields(ABC):
    def __init_subclass__(cls, subclass: Any):
        cls.subclass = subclass
        cls.__init_subclass__ = lambda: None
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for name, t in inspect.get_annotations(type(self)).items():
            if issubclass(t, type(self).subclass):
                setattr(self, name, t(self, name))

class MongoDatabase(Database, ImplicitClassFields, ABC, subclass=MongoCollection):
    pass

class MongoInstance(MongoClient, ImplicitClassFields, ABC, subclass=MongoDatabase):
    pass