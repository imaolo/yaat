from __future__ import annotations
from beanie.odm.actions import Insert, Update, Replace, Delete
from beanie import Document, before_event
from pydantic import BaseModel
from typing import ClassVar
from abc import ABC

class DocUIMetadata(BaseModel):
    schema: dict
    createable: bool
    updateable: bool
    deleteable: bool
    # TODO field specific?

class Doc(Document, ABC):
    createable: ClassVar[bool] = True
    readable: ClassVar[bool] = True
    updateable: ClassVar[bool] = True
    deleteable: ClassVar[bool] = True
    # TODO field specific?

    @before_event(Insert)
    def create_event(self):
        if not self.createable: raise RuntimeError()

    @before_event(Update, Replace)
    def update_event(self):
        if not self.updateable: raise RuntimeError()

    @before_event(Delete)
    def delete_event(self):
        if not self.deleteable: raise RuntimeError()

    @classmethod
    def get_metadata(cls) -> DocUIMetadata:
        # NOTE implement in sub-class to differentiate between system-wide and UI restrictions.
        return DocUIMetadata(
            schema=cls.model_json_schema(),
            createable=cls.createable,
            updateable=cls.updateable,
            deleteable=cls.deleteable,
        )

    @classmethod
    async def init(cls):
        pass

class ReadOnlyDoc(Doc, ABC):
    createable: ClassVar[bool] = False
    updateable: ClassVar[bool] = False
    deleteable: ClassVar[bool] = False
