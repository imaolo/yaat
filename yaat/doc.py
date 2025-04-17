from __future__ import annotations
from beanie import Document, before_event, Insert, Update, Replace, Delete, PydanticObjectId
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, Body
from typing import ClassVar, TypeVar, Generic, Any
from dataclasses import dataclass
from abc import ABC
import functools, traceback

class IdView(BaseModel):
    id: PydanticObjectId | str = Field(alias='_id')

@dataclass  
class DocArgs:
    schema_createable: bool = True,
    schema_readable: bool = True,
    schema_updateable: bool = True,
    schema_deleteable: bool = True,
    db_createable: bool = True,
    db_updateable: bool = True,
    db_deleteable: bool = True

class DocUIMetadata(BaseModel):
    create: dict | None = None
    read: dict | None = None
    update: dict | None = None
    delete: dict | None = None

class UIMetadataField:
    def __init__(self):
        self.metadata = None

    def __get__(self, instance: Doc, owner: type[Doc]) -> DocUIMetadata:
        if self.metadata is None:
            self.metadata = DocUIMetadata(
                create=owner.schema_create() if owner.doc_args.schema_createable else None,
                read=owner.schema_read() if owner.doc_args.schema_readable else None,
                update=owner.schema_update() if owner.doc_args.schema_updateable else None,
                delete=owner.schema_delete() if owner.doc_args.schema_deleteable else None,
            )
        return self.metadata

class AggregationPayload(BaseModel):
    filter: dict[str, Any] | None = {}
    sort: list[tuple[str, int]] | None = []
    skip: int = 0
    limit: int = 10
    rowGroupCols: list[str] = []
    groupKeys: list[str]
    count: bool = False

DocType = TypeVar('DocType')
class CRUDReadRes(BaseModel, Generic[DocType]):
    items: list[DocType]
    total: int

class Doc(Document, ABC):
    ui_metadata: ClassVar[DocUIMetadata] = None # NOTE place holder
    # TODO field specific crud'able? right now controlled by schema, not at db level

    def __init_subclass__(cls, *args, doc_args: DocArgs, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        cls.doc_args = doc_args
        cls.ui_metadata = UIMetadataField() # NOTE fill in placeholder

    # triggers TODO it would be nice to have these triggers overloadable like they are for crud endpoints
    # current they requre the decorator in the child class

    @before_event(Insert)
    async def before_create_trg(self):
        if not self.doc_args.db_createable: raise RuntimeError()

    @before_event(Update, Replace)
    async def before_update_trg(self):
        if not self.doc_args.db_updateable: raise RuntimeError()

    @before_event(Delete)
    async def before_delete_trg(self):
        if not self.doc_args.db_deleteable: raise RuntimeError(type(self))

    # json schema generation

    @classmethod
    def model_json_schema(cls, *args, exclude: set = {}, **kwargs) -> dict:
        return cls.remove_from_schema(super().model_json_schema(*args, **kwargs), exclude)

    @classmethod
    def remove_from_schema(cls, schema: dict, fields: set = {}) -> dict:
        for name, _def in schema.get('$defs', {}).items():
            if 'type' not in _def or _def['type'] != 'object':
                continue
            schema['$defs'][name] = cls.remove_from_schema(_def, fields)
        for field in fields:
            schema['properties'].pop(field, None)
            if 'required' in schema and field in schema['required']:
                schema['required'].remove(field)
        return schema

    # json crud schemas

    @classmethod    
    def schema_create(cls) -> dict | None:
        return cls.model_json_schema()

    @classmethod
    def schema_read(cls) -> dict | None:
        return cls.model_json_schema()

    @classmethod
    def schema_update(cls) -> dict | None:
        return cls.model_json_schema()

    @classmethod
    def schema_delete(cls) -> dict | None:
        return cls.model_json_schema()

    # crud endpoints handlers

    @classmethod
    async def crud_c(cls, doc: dict) -> None:
        await cls(**doc).create()

    # crud read helper
    @staticmethod
    def generate_group_stages(cols: list[str], keys: list[str]) -> list[dict]:
        pipe = []
        if not cols and not keys:
            return pipe

        if (d := (lcols:=len(cols)) - len(keys)) > 0:
            distinct_field = cols[lcd:=(lcols - d)]
            pipe.append({
                '$group':{
                    '_id': f"${distinct_field}"
                }
            })
            pipe.append({
                '$project':{
                    distinct_field: "$_id",
                    **{of:ov for (of, ov) in zip(cols[:lcd], keys)},
                    'group': {'$literal': True},
                    '_id': {
                        '$function': {
                            'body': 'function() { return new ObjectId(); }',
                            'args': [],
                            'lang': 'js'
                        }
                    }
                }
            })
        elif d == 0:
            pipe.append({
                '$match': {k:v for k, v in zip(cols, keys)}
            })
        else:
            raise RuntimeError(cols, keys)

        return pipe

    @classmethod
    async def crud_r(cls, payload: AggregationPayload = Body(...)) -> list[Doc] | int :
        pipeline = [
            {'$match': payload.filter},
            *([{ "$sort": dict(payload.sort) }] if payload.sort and not payload.count else []),
            *cls.generate_group_stages(payload.rowGroupCols, payload.groupKeys)
        ]

        # docs vs count
        if not payload.count:
            pipeline.extend([
                {"$skip": payload.skip},
                {"$limit": payload.limit},
            ])
        else:
            pipeline.append({'$count': 'total'})


        # TODO custom validation/projection
        data = await cls.aggregate(pipeline).to_list()
        return data[0]['total'] if payload.count else list(map(lambda d: cls(**d), data))

    @classmethod
    async def crud_u(cls, doc: Doc) -> Doc:
        raise RuntimeError("TODO not implemented")

    @classmethod
    async def crud_d(cls, payload: AggregationPayload = Body(...)) -> str:
        # NOTE - ignore everything except filter
        # TODO - handle groups
        return str(await cls.find(payload.filter).delete_many())

    # configure endpoints

    @classmethod
    def add_crud(cls, router: APIRouter):

        def wrap_endpoint(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except HTTPException:
                    raise
                except Exception as e:
                    traceback.print_exc()
                    raise HTTPException(status_code=500, detail=str(e))
            return async_wrapper

        # TODO gate these
        router.add_api_route(
            '/'+cls.__name__,
            endpoint=wrap_endpoint(cls.crud_c),
            methods=["POST"],
        )

        router.add_api_route(
            '/read/'+cls.__name__,
            endpoint=wrap_endpoint(cls.crud_r),
            methods=["POST"],
            response_model=list[cls] | int
        )

        router.add_api_route(
            '/'+cls.__name__,
            endpoint=wrap_endpoint(cls.crud_u),
            methods=["PUT"]
        )

        router.add_api_route(
            '/delete/'+cls.__name__,
            endpoint=wrap_endpoint(cls.crud_d),
            methods=["POST"]
        )