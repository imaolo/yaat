from __future__ import annotations
from beanie import Document, before_event, Insert, Update, Replace, Delete 
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Body
from fastapi_paginate import Page, Params, create_page
from typing import ClassVar
from dataclasses import dataclass
from abc import ABC

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

    @classmethod
    async def crud_r(cls, params: Params = Depends()) -> Page[Doc]:
        docs = await cls.find().skip((params.page-1) * params.size).limit(params.size).to_list()
        return create_page(
            items=docs,
            total=await cls.find().count(),
            params=params
        )

    @classmethod
    async def crud_u(cls, doc: Doc) -> Doc:
        raise RuntimeError("TODO not implemented")

    @classmethod
    async def crud_d(cls, ids: list[str] = Body(...)):
        await cls.find_many({"_id": {"$in": ids}}).delete()

    # configure endpoints

    @classmethod
    def add_crud(cls, router: APIRouter):
        router.add_api_route(
            '/'+cls.__name__,
            endpoint=cls.crud_c,
            methods=["POST"],
        )

        router.add_api_route(
            '/'+cls.__name__,
            endpoint=cls.crud_r,
            methods=["GET"],
            response_model=Page[cls]
        )

        router.add_api_route(
            '/'+cls.__name__,
            endpoint=cls.crud_u,
            methods=["PUT"]
        )

        router.add_api_route(
            '/'+cls.__name__,
            endpoint=cls.crud_d,
            methods=["DELETE"]
        )

read_only_doc_args = doc_args=DocArgs(schema_createable=False, schema_deleteable=False, schema_updateable=False)