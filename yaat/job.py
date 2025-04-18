from __future__ import annotations
from yaat.state import State
from yaat.doc import Doc, DocArgs
from apscheduler.util import utc_timestamp_to_datetime
from beanie.operators import Eq
from pydantic import field_serializer, model_validator, Field, BaseModel
from pydantic.json_schema import SkipJsonSchema, WithJsonSchema
from typing import ClassVar, Any, Annotated
from datetime import datetime, timezone
import base64

# TODO - better functions

aps_job_doc_args = DocArgs(
    db_createable=False,
    db_updateable=False,
    db_deleteable=False,
    schema_createable=False,
    schema_updateable=False,
)
class APSJobDoc(Doc, doc_args=aps_job_doc_args):
    # fields

    # TODO control schema skip but only for certain crud's
    job_type: Annotated[str | None, WithJsonSchema({'type':'string'})] = Field(default=None, init=False)
    next_run_time: float | None = Field(default=None, init=False)
    id: SkipJsonSchema[str | None] = Field(default=None, init=False, alias='_id')
    job_state: SkipJsonSchema[bytes | None] = Field(default=None, init=False)
    add_job_args: SkipJsonSchema[list[Any] | None] = Field(default=None, exclude=True)
    add_job_kwargs: SkipJsonSchema[dict[str, Any] | None] = Field(default=None, exclude=True)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # field descriptors

    aps_fields: ClassVar[list[str]] = ['next_run_time', 'id', 'job_state']
    add_job_fields: ClassVar[list[str]] = ['add_job_args', 'add_job_kwargs']

    # config

    class Settings:
        name = 'APSJobDoc'
 
    model_config = {
        "extra": "allow",
    }

    # json schema

    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        schema = super().model_json_schema(*args, **kwargs)
        if 'properties' in schema and 'next_run_time' in schema['properties']:
            schema['properties']['next_run_time']['format'] = 'date-time'
            schema['properties']['next_run_time']['type'] = 'string'
        return schema

    @classmethod
    def schema_create(cls, *args, **kwargs):
        return cls.model_json_schema(exclude=set(cls.aps_fields + ['job_type', 'created_at']))

    @classmethod
    def schema_read(cls) -> dict:
        return cls.model_json_schema(exclude={'job_type'} if cls is not APSJobDoc else {})


    # db overloads

    async def insert(self, *, session):
        # some fields must be null on insertion
        for field in self.model_fields.keys():
            if field in self.aps_fields and (value:=getattr(self, field)) is not None:
                raise TypeError(f"aps field ({field=}, {value=}) is managed internally and cannot be manually set")

        # create and get the job doc
        job = State.scheduler.add_job(*([self.func] + self.add_job_args), **self.add_job_kwargs)
        job_doc = await APSJobDoc.find_one(Eq("_id", job.id), session=session)

        # update and return doc
        await job_doc.get_motor_collection().update_one({"_id": job_doc.id}, {"$set": self.get_update_doc()})
        return await type(self).get(job_doc.id, session=session)

    @classmethod
    def find(cls, *args, **kwargs):
        return super().find(*((cls.job_type_filter(), )+args), *args, **kwargs)

    @classmethod
    def find_one(cls, *args, **kwargs):
        return super().find_one(*((cls.job_type_filter(), )+args), **kwargs)

    @classmethod
    def find_many(cls, *args, **kwargs):
        return super().find_many(*((cls.job_type_filter(), )+args), **kwargs)

    async def delete(self, *args, **kwargs):
        State.scheduler.remove_job(self.id)

    @classmethod
    async def delete_all(cls, *args, **kwargs):
        for jobdoc in await cls.find().to_list():
            State.scheduler.remove_job(jobdoc.id)

    # helpers

    def get_update_doc(self, update_doc: dict | None = None) -> dict:
        if update_doc is None:
            update_doc = {}

        update_doc.update({
            'add_job_args': None,
            'add_job_kwargs': None,
            'job_type': type(self).__name__
        })

        for field in self.model_fields.keys():
            if field not in self.aps_fields \
                and field not in self.add_job_fields \
                    and update_doc.get(field, None) is None:
                update_doc[field] = val.model_dump() if isinstance(val:=getattr(self, field), BaseModel) else val
        return update_doc

    @classmethod
    def job_type_filter(cls):
        return Eq("job_type", cls.__name__) if cls is not APSJobDoc else {}

    # job function to execute

    async def func(self):
        pass

class IntervalJobDoc(APSJobDoc, doc_args=DocArgs(schema_readable=False)):
    seconds: int = 60*60

    @model_validator(mode='before')
    @classmethod
    def set_add_job_args(cls, vals):
        vals['add_job_args'] = ['interval']
        vals['add_job_kwargs'] = {'seconds': vals['seconds']}
        vals['coalesce'] = True
        vals['misfire_grace_time'] = 0
        return vals

    async def func(self):
        raise NotImplementedError()