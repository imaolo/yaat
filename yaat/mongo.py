from __future__ import annotations
from bson import Int64, ObjectId
from datetime import datetime
from dataclasses import dataclass, fields, asdict
from typing import get_origin, get_args, Callable, Union, ClassVar, Any, final
from abc import ABC, ABCMeta
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

@dataclass(kw_only=True)
class MongoDoc(ABC):
    schema: ClassVar[SCHEMA_DICT_T]
    colldoc: ClassVar[CollDoc]
    fields: ClassVar[dict[str, Any]]

    @classmethod
    def __init_subclass__(cls, colldoc: CollDoc | None = None, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
    
        # each sub class is the same
        dataclass(cls, kw_only=True)

        # extract the collname
        cls.colldoc = colldoc

        # set the type hints (skip class vars)
        g = sys.modules[cls.__module__].__dict__
        cls.fields = {
            field.name: eval(field.type, g) if isinstance(field.type, str) else field.type
            for field in fields(cls)
            if not(isinstance(field.type, str) and field.type.startswith('ClassVar'))
        } 

        # set the schema
        (schema:=Py2BSON_Schema[cls])['properties'].update({'_id': {'bsonType': 'objectId'}})
        cls.schema = {'$jsonSchema': schema}

    # type check / implicit conversion
    def __post_init__(self):
        for name, t in self.fields.items():
            if Py2BSON_Schema.is_primitive(t):
                if not isinstance(val:=getattr(self, name), t) and not isinstance(val ,type):
                    setattr(self, name, t(val))

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

class MongoCollectionMeta(ABCMeta):
    def __subclasscheck__(cls, subclass):
        if ABCMeta.__subclasscheck__(cls, subclass): return True
        if not subclass.__base__ == MongoCollection: return False
        return issubclass(subclass.doct, cls.doct)

class MongoCollection(Collection, ABC, metaclass=MongoCollectionMeta):
    doct: ClassVar[type[MongoDoc]]

    def __init_subclass__(cls, doct: type[MongoDoc], *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        cls.doct = doct

    @classmethod
    def __class_getitem__(cls, k: type[Any]) -> type[MongoCollection] | None:
        if not issubclass(k, MongoDoc): raise TypeError()
        class _MongoCollection(MongoCollection, doct=k):
            pass
        return _MongoCollection

    def __init__(self, *args, **kwargs):
        super().__init__(*args, *kwargs)

        if self.name in self.database.list_collection_names():
            if self.options().get('validator') != self.doct.schema:
                raise RuntimeError(f"illegal attempt to update schema {self.name}")
        else:
            self.database.create_collection(self.name, validator=self.doct.schema)

        if self.doct.colldoc:
            self.create_index(*self.doct.colldoc.index.args, **self.doct.colldoc.index.kwargs)

        # TODO time series

class ImplicitClassFields(ABC):
    def __init_subclass__(cls, superclass: type[Any], *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        cls.fields: dict[str, type[Any]] = {name: t for name, t in inspect.get_annotations(cls).items()
                                            if issubclass(t, superclass)}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for name, t in self.fields.items():
            setattr(self, name, t(self, name))

class MongoDatabase(ImplicitClassFields, Database, ABC, superclass=MongoCollection):
    def __init_subclass__(cls, superclass: type[Any] = MongoCollection, *args, **kwargs):
        super().__init_subclass__(superclass=superclass, *args, **kwargs)

class MongoInstance(ImplicitClassFields, MongoClient, ABC, superclass=MongoDatabase):
    def __init_subclass__(cls, superclass: type[Any] = MongoDatabase, *args, **kwargs):
        super().__init_subclass__(superclass=superclass, *args, **kwargs)