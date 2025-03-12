from __future__ import annotations
from yaat.doc import ReadOnlyDoc, Doc
from yaat.helpers import getenv
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.job import Job
from pydantic import PrivateAttr
from typing import ClassVar, Generic, TypeVar
from beanie import Document, init_beanie
from abc import ABC, abstractmethod

DOCKER = getenv('DOCKER', False)

class APSJobDoc(ReadOnlyDoc):
    id: str
    next_run_time: float
    job_state: bytes
    _job: Job = PrivateAttr()
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._job = self.deserialize(self.job_state)

    @staticmethod
    def deserialize(job_state: bytes) -> Job:
        return State.jobstore._reconstitute_job(job_state)

T = TypeVar('T')
class StateField(ABC, Generic[T]):
    value: T | None = None

    def __get__(self, instance: None, owner: State) -> T:
        if self.value is None: self.value = self.get_value(instance, owner)
        return self.value

    @abstractmethod
    def get_value(self, instance: None, owner: State) -> T: pass

class ClientField(StateField[AsyncIOMotorClient]):
    def get_value(self, instance: None, owner: State) -> AsyncIOMotorClient:
        return AsyncIOMotorClient(f"mongodb://{'mongo' if DOCKER else 'localhost'}:27017")

class JobStoreField(StateField[MongoDBJobStore]):
    def get_value(self, instance: None, owner: State) -> MongoDBJobStore:
        return MongoDBJobStore(client=owner.client.delegate, database='yaatdb', collection=APSJobDoc.__name__)

class SchedulerField(StateField[BackgroundScheduler]):
    def get_value(self, instance: None, owner: State) -> BackgroundScheduler:
        return BackgroundScheduler(jobstores={'default': owner.jobstore})

class State(ABC):
    client: ClassVar[AsyncIOMotorClient] = ClientField()
    scheduler: ClassVar[BackgroundScheduler] = SchedulerField()
    jobstore: ClassVar[MongoDBJobStore] = JobStoreField()
    document_models: ClassVar[set[type[Doc]]] = {APSJobDoc}

    @classmethod
    async def init(cls):
        await init_beanie(database=State.client['yaatdb'], document_models=cls.document_models)
        for model in cls.document_models: model.init()
        cls.scheduler.start()

    @classmethod
    def terminate(cls, *document_models: list[type[Document]]):
        cls.scheduler.shutdown(wait=True)

    def __init_subclass__(cls):
        raise TypeError(cls)
