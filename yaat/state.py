from __future__ import annotations
from yaat.doc import Doc, DocUIMetadata
from yaat.helpers import getenv
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from beanie import init_beanie
from pydantic import BaseModel
from typing import ClassVar, Generic, TypeVar
from abc import ABC, abstractmethod
from fastapi import APIRouter

DOCKER = getenv('DOCKER', False)

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
        return MongoDBJobStore(client=owner.client.delegate, database='yaatdb', collection='APSJobDoc') # NOTE APSJobDoc hardcoded

class SchedulerField(StateField[AsyncIOScheduler]):
    def get_value(self, instance: None, owner: State) -> AsyncIOScheduler:
        return AsyncIOScheduler(jobstores={'default': owner.jobstore})

class State(ABC):
    client: ClassVar[AsyncIOMotorClient] = ClientField()
    scheduler: ClassVar[AsyncIOScheduler] = SchedulerField()
    jobstore: ClassVar[MongoDBJobStore] = JobStoreField()
    document_models: ClassVar[list[type[Doc]]] # NOTE set before init()
    ui_metadatas: ClassVar[list[DocUIMetadata]] # NOTE placeholder

    @classmethod
    async def init_beanie(cls):
        await init_beanie(database=State.client['yaatdb'], document_models=cls.document_models)

    @classmethod
    def init_scheduler(cls):
        cls.scheduler.start()

    @classmethod
    def init_ui_metadatas(cls):
        cls.ui_metadatas: list[DocUIMetadata] = [model.ui_metadata for model in State.document_models]

    @classmethod
    async def init(cls):
        await cls.init_beanie()
        cls.init_scheduler()
        cls.init_ui_metadatas()

    @classmethod
    def terminate(cls):
        cls.scheduler.shutdown(wait=True)
        cls.client = ClientField()
        cls.scheduler = SchedulerField()
        cls.jobstore = JobStoreField()

    @classmethod
    def get_router(cls) -> APIRouter:
        router = APIRouter()

        @router.get('/metadatas', response_model=list[DocUIMetadata])
        def mds() -> list[DocUIMetadata]:
            return cls.ui_metadatas

        for schema in cls.document_models:
            schema.add_crud(router)

        return router
