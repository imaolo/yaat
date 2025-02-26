from __future__ import annotations
from yaat.mongo import MongoDoc, CollDoc, IndexDoc
from yaat.helpers import getenv, wait_until_true
import unittest, pytest, docker, abc

LISTEN_SERVICES = {
    'mongo': getenv('LISTEN_MONGO', True),
    'yaat': getenv('LISTEN_YAAT', True)
}
START_SERVICES = getenv('START_SERVICES', True)

class IntegrationTestCase(unittest.TestCase, abc.ABC):
    def __init_subclass__(cls, start_services: bool = START_SERVICES, services: dict[str, bool] = {}, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        cls.docker_ip = None
        cls.services = LISTEN_SERVICES | services
        if start_services:
            cls.pytestmark = pytest.mark.usefixtures('init_class_docker')
        else:
            cls.listen_services()

    @classmethod
    def listen_services(cls):
        containers = docker.from_env().containers
        for name, listen in cls.services.items():
            if listen:
                container, = containers.list(filters={"label": f"com.docker.compose.service={name}"})
                def check():
                    container.reload()
                    return container.health == 'healthy'
                wait_until_true(check, 60, 0.1, msg=name)

class Doc1(MongoDoc):
    f1: str
    f2: int

class Doc2(MongoDoc):
    d1: Doc1
    f1: int

class Doc1Array(MongoDoc):
    l1: list[str]
    l2: list[list[int]]
    f3: int

class Doc2Array(MongoDoc):
    l1: list[Doc1Array]
    l2: list[list[Doc1Array]]
    l3: list[float]
    f1: bool

class Doc1Dict(MongoDoc):
    dict1: dict
    f1: int

class Doc1TypeDoc(MongoDoc):   
    f1: int
    t1: type[MongoDoc]

class Doc1EmptyIndexDoc(MongoDoc, colldoc=CollDoc(index=IndexDoc(args=[], kwargs={}))):
    f1: int
    t1: type[Doc1]