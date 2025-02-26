from __future__ import annotations
from yaat.mongo import MongoDoc, CollDoc, IndexDoc
from yaat.helpers import getenv, wait_until_true
from pymongo import MongoClient
from abc import ABC
import pytest, unittest, docker

LISTEN_SERVICES = {
    'mongo': getenv('LISTEN_MONGO', True),
    'yaat': getenv('LISTEN_YAAT', True)
}
START_SERVICES = getenv('START_CONTAINERS', True)

def create_integration_test_class(start_services: bool=START_SERVICES, listen_services: dict[str, bool]={}):
    class IntegrationTestCase(unittest.TestCase, ABC):
        if start_services:
            @classmethod
            @pytest.fixture(autouse=True)
            def inject_docker_services(cls, docker_ip, docker_services):
                cls.docker_ip = docker_ip
                cls.docker_services = docker_services

        @classmethod
        def setUpClass(cls):
            super().setUpClass()
            containers = docker.from_env().containers
            for name, listen in (LISTEN_SERVICES | listen_services).items():
                if listen:
                    container, = containers.list(filters={"label": f"com.docker.compose.service={name}"})
                    def check():
                        container.reload()
                        return container.health == 'healthy'
                    wait_until_true(check, 60, 0.1, msg=name)
            cls.dbc = MongoClient(host=cls.docker_ip, port=cls.docker_services.port_for("mongo", 27017)) if start_services else MongoClient()
    return IntegrationTestCase

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