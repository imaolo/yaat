from __future__ import annotations
from bson import Int64, ObjectId
from datetime import datetime
from dataclasses import dataclass, fields, asdict
from typing import TYPE_CHECKING, get_origin, get_args, Callable, Union, ClassVar, Any
import abc, sys
if TYPE_CHECKING:
    from pymongo.synchronous.database import Collection, Database

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

@dataclass(frozen=True, kw_only=True)
class MongoDoc(abc.ABC):
    schema: ClassVar[SCHEMA_DICT_T]
    collname: ClassVar[str]
    colldoc: ClassVar[CollDoc]
    typehints: ClassVar[dict[str, Any]]

    @classmethod
    def __init_subclass__(cls, coll: CollDoc | None = None, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
    
        # each sub class is the same
        dataclass(cls, kw_only=True, frozen=True)

        # set the type hints (skip class vars)
        g = sys.modules[cls.__module__].__dict__
        cls.typehints = {
            field.name: eval(field.type, g) if isinstance(field.type, str) else field.type
            for field in fields(cls)
            if not(isinstance(field.type, str) and field.type.startswith('ClassVar'))
        }

        # check the data fields
        for th in cls.typehints.values():
            if cls.is_primitive(th): continue
            if cls.is_mongodoc(th): continue
            if cls.is_list(th): continue
            if cls.is_optional(th): continue
            if cls.is_mongodoc_type(th): continue
            raise RuntimeError(f"invalid MongoDoc class {cls=}, {th=}")

        # extract the collname
        cls.collname = cls.__name__
        cls.colldoc = coll

        # set the schema
        (schema:=Py2BSON_Schema[cls])['properties'].update({'_id': {'bsonType': 'objectId'}})
        cls.schema = {'$jsonSchema': schema}

    @classmethod
    def create_collection(cls, db: Database) -> Collection:
        coll = db[cls.collname]

        # schema
        if cls.collname in db.list_collection_names():
            db.command('collMod', cls.collname, validator=cls.schema)
        else:
            db.create_collection(cls.collname, validator=cls.schema)
        coll = db[cls.collname]

        # create index
        if cls.colldoc:
            coll.create_index(*cls.colldoc.index.args, **cls.colldoc.index.args)

        # TODO time series information

        return coll

    @property
    def dict(self) -> DOC_DICT_T: return asdict(self)

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


PyBSONType = PyBSONPrimType | type[MongoDoc] | type[list] | type[Union['PyBSONType', None]]
class _Py2BSON_Schema:
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
                'properties': {name: self[type] for name, type in t.typehints.items()},
                'required':  list(t.typehints.keys()),
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

class CollDoc(MongoDoc):
    index: int

class IndexDoc(MongoDoc):
    args: list
    kwargs: dict
