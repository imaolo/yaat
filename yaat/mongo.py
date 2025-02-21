from __future__ import annotations
from bson import Int64, ObjectId
from datetime import datetime
from dataclasses import dataclass, fields, asdict, field
from typing import TYPE_CHECKING, get_origin, get_args, get_type_hints, Callable, Union, ClassVar, Any
import abc
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
    colldoc: ClassVar[type[MongoDoc]]

    @classmethod
    def __init_subclass__(cls, colldoc: MongoCollDoc = None, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
    
        # each sub class is the same
        dataclass(cls, kw_only=True, frozen=True)

        # check that field types are valid
        type_hints = get_type_hints(cls)
        for field in fields(cls):
            th = type_hints[field.name]
            if cls.is_primitive(th): continue
            if cls.is_mongodoc(th): continue
            if cls.is_list(th): continue
            if cls.is_optional(th): continue
            if cls.is_mongodoc_type(th): continue
            raise RuntimeError(f"invalid MongoDoc class {cls}, {field}, {th}")

        # extract the collname
        cls.collname = cls.__name__
        cls.colldoc = colldoc # TODO

        # set the schema
        (schema:=Py2BSON_Schema[cls])['properties'].update({'_id': {'bsonType': 'objectId'}})
        cls.schema = {'$jsonSchema': schema}

    @classmethod
    def create_collection(cls, db: Database) -> Collection:
        # schema
        if cls.collname in db.list_collection_names():
            db.command('collMod', cls.collname, validator=cls.get_schema())
        else:
            db.create_collection(cls.collname, validator=cls.get_schema())

        # TODO time series information
        # TODO index information

        return db[cls.collname]

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
        type_hints = get_type_hints(t)
        return {'bsonType': 'object',
                'properties': {field.name: self[type_hints[field.name]] for field in fields(t)},
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

    def get_mongodoc_type(self, _: type[type[MongoDoc]]): return {'bsonType': 'str'}

Py2BSON_Schema = _Py2BSON_Schema()

# TODO
class MongoCollDoc(MongoDoc):
    index_information: str