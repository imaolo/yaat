from __future__ import annotations
from beanie import Document, Link, before_event
from beanie.odm.actions import Insert, Update, Replace, Delete 
from pydantic import BaseModel, create_model
from pydantic.fields import FieldInfo
from typing import ClassVar, Union, get_origin, get_args
from fastapi import APIRouter, Depends
from fastapi_paginate import Page, Params, create_page
from abc import ABC

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
                create=owner.schema_create(),
                read=owner.schema_read(),
                update=owner.schema_update(),
                delete=owner.schema_delete(),
            )
        return self.metadata

class LinksEmbeddedDocTypeField:
    def __init__(self):
        self.doctype = None

    @staticmethod
    def get_doc_from_info(field_info: FieldInfo) -> type[Doc]:
        org = get_origin(field_info.annotation)
        args = get_args(field_info.annotation)

        if org not in (Link, Union): return field_info.annotation

        if org is Link: return args[0]

        assert org is Union, org
        if len(args) != 2: return field_info.annotation
        if args[1] is not type(None): return field_info.annotation
        if get_origin(args[0]) is not Link: return field_info.annotation
        return get_args(args[0])[0]

    def __get__(self, instance: Doc, owner: type[Doc]) -> type[Doc]:
        if self.doctype is None:
            fields = {name: (self.get_doc_from_info(field), ...) for name, field in owner.model_fields.items()}
            self.doctype = create_model(f"{owner.__name__}__Flattened", **fields)
        return self.doctype

class Doc(Document, ABC):
    createable: ClassVar[bool] = True
    readable: ClassVar[bool] = True
    updateable: ClassVar[bool] = True
    deleteable: ClassVar[bool] = True
    ui_metadata: ClassVar[DocUIMetadata] = None # NOTE place holder
    links_embedded: ClassVar[type[BaseModel]] = None # NOTE place holder
    # TODO field specific?

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.ui_metadata = UIMetadataField() # NOTE placeholder filled
        cls.links_embedded = LinksEmbeddedDocTypeField() # NOTE placeholder filled

    @before_event(Insert)
    async def before_create_trg(self):
        if not self.createable: raise RuntimeError()

    @before_event(Update, Replace)
    async def before_update_trg(self):
        if not self.updateable: raise RuntimeError()

    @before_event(Delete)
    async def before_delete_trg(self):
        if not self.deleteable: raise RuntimeError(type(self))

    @classmethod
    def remove_from_schema(cls, schema: dict | None, *fields: list[str] | None) -> dict | None:
        if schema is None: return None

        for name, _def in schema.get('$defs', {}).items():
            if 'type' not in _def or _def['type'] != 'object':
                continue
            schema['$defs'][name] = cls.remove_from_schema(_def, *fields)

        for field in fields:
            schema['properties'].pop(field, None)
            if 'required' in schema and field in schema['required']:
                schema['required'].remove(field)
        return schema

    @classmethod
    def get_links_embedded_schema(cls) -> dict:
        return cls.remove_from_schema(cls.links_embedded.model_json_schema(), '_id', 'revision_id', 'id', 'apsjob')

    @classmethod    
    def schema_create(cls) -> dict | None:
        return cls.get_links_embedded_schema() if cls.createable else None

    @classmethod
    def schema_read(cls) -> dict | None:
        return cls.get_links_embedded_schema() if cls.readable else None

    @classmethod
    def schema_update(cls) -> dict | None:
        return cls.get_links_embedded_schema() if cls.updateable else None

    @classmethod
    def schema_delete(cls) -> dict | None:
        return cls.get_links_embedded_schema() if cls.deleteable else None

    @classmethod
    async def init(cls):
        pass

    @classmethod
    async def crud_c(cls, doc: dict) -> None:
        await cls(**doc).create()

    @classmethod
    async def crud_r(cls, params: Params = Depends()) -> Page[Doc]:
        docs = await cls.find().skip((params.page-1) * params.size).limit(params.size).to_list()
        [await doc.fetch_all_links() for doc in docs]
        return create_page(
            items=docs,
            total=await cls.find().count(),
            params=params
        )

    @classmethod
    async def crud_u(cls, doc: Doc) -> Doc:
        raise RuntimeError("TODO not implemented")

    @classmethod
    async def crud_d(cls, id: str) -> Doc:
        doc = await cls.get(id)
        await doc.delete()
        return doc

    @classmethod
    def add_crud(cls, router: APIRouter):
        router.add_api_route(
            '/'+cls.__name__+"__Flattened",
            endpoint=cls.crud_c,
            methods=["POST"],
        )

        router.add_api_route(
            '/'+cls.__name__+"__Flattened",
            endpoint=cls.crud_r,
            methods=["GET"],
            response_model=Page[cls]
        )

        router.add_api_route(
            '/'+cls.__name__+"__Flattened",
            endpoint=cls.crud_u,
            methods=["PUT"]
        )

        router.add_api_route(
            '/'+cls.__name__+"__Flattened",
            endpoint=cls.crud_d,
            methods=["DELETE"]
        )

class ReadOnlyDoc(Doc, ABC):
    createable: ClassVar[bool] = False
    updateable: ClassVar[bool] = False
    deleteable: ClassVar[bool] = False
