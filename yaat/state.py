from __future__ import annotations
from yaat.doc import ReadOnlyDoc, Doc, DocUIMetadata
from yaat.helpers import getenv
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.util import utc_timestamp_to_datetime
from beanie import Link, init_beanie
from beanie.odm.actions import before_event, Insert, Update, Replace, Delete
from pydantic import Field, field_serializer
from typing import ClassVar, Generic, TypeVar, Optional
from abc import ABC, abstractmethod
from fastapi import APIRouter
import base64

DOCKER = getenv('DOCKER', False)

class APSJobDoc(ReadOnlyDoc):

    id: str
    next_run_time: float
    job_state: bytes

    @field_serializer('job_state')
    def serialize_job_state(self, job_state: bytes) -> str:
        return base64.b64encode(job_state).decode("ascii")

    @field_serializer('next_run_time')
    def serialize_next_run_time(self, next_run_time: float) -> str:
        return str(utc_timestamp_to_datetime(next_run_time))

    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        schema = super().model_json_schema(*args, **kwargs)
        schema['properties']['next_run_time']['format'] = 'date-time'
        schema['properties']['next_run_time']['type'] = 'string'
        return schema

class JobDoc(Doc, ABC):
    apsjob: Optional[Link[APSJobDoc]] = Field(None, exclude=True)

    # # TODO - do these programmatically in init_subclass
    # @before_event(Insert)
    # def before_create_trg(self):
    #     super().before_create_trg()
    #     raise NotImplementedError(self)

    @before_event(Update, Replace)
    async def before_update_trg(self):
        super().before_update_trg()
        raise NotImplementedError(self)

    @before_event(Delete)
    async def before_delete_trg(self):
        await super().before_delete_trg()
        # raise NotImplementedError(self) TODO 

    async def __call__(self):
        raise NotImplementedError(self)

    @classmethod    
    def schema_create(cls) -> dict | None:
        return cls.remove_from_schema(super().schema_create(), 'apsjob')

T = TypeVar('T')
class StateField(ABC, Generic[T]):
    value: T | None = None

    def __get__(self, instance: None, owner: State) -> T:
        if self.value is None:
            self.value = self.get_value(instance, owner)
        return self.value

    @abstractmethod
    def get_value(self, instance: None, owner: State) -> T: pass

class ClientField(StateField[AsyncIOMotorClient]):
    def get_value(self, instance: None, owner: State) -> AsyncIOMotorClient:
        return AsyncIOMotorClient(f"mongodb://{'mongo' if DOCKER else 'localhost'}:27017")

class JobStoreField(StateField[MongoDBJobStore]):
    def get_value(self, instance: None, owner: State) -> MongoDBJobStore:
        return MongoDBJobStore(client=owner.client.delegate, database='yaatdb', collection=APSJobDoc.__name__)

class SchedulerField(StateField[AsyncIOScheduler]):
    def get_value(self, instance: None, owner: State) -> AsyncIOScheduler:
        return AsyncIOScheduler(jobstores={'default': owner.jobstore})

class State(ABC):
    client: ClassVar[AsyncIOMotorClient] = ClientField()
    scheduler: ClassVar[AsyncIOScheduler] = SchedulerField()
    jobstore: ClassVar[MongoDBJobStore] = JobStoreField()
    document_models: ClassVar[list[type[Doc]]] # NOTE: set before init()

    @classmethod
    async def init(cls):
        await init_beanie(database=State.client['yaatdb'], document_models=cls.document_models)
        for model in cls.document_models:
            await model.init()
        cls.scheduler.start()
        cls.ui_metadatas: list[DocUIMetadata] = [model.ui_metadata for model in State.document_models]

    @classmethod
    def terminate(cls):
        cls.scheduler.shutdown(wait=True)

    @classmethod
    def get_router(cls) -> APIRouter:
        router = APIRouter()

        @router.get('/metadatas', response_model=list[DocUIMetadata])
        def mds() -> list[DocUIMetadata]:
            return cls.ui_metadatas

        for schema in cls.document_models:
            schema.add_crud(router)

        return router
