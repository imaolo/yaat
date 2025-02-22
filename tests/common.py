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
START_CONTAINERS = getenv('START_CONTAINERS', True)

def create_integration_test_class(start_services: bool=START_CONTAINERS, listen_services: dict[str, bool]=LISTEN_SERVICES):
    class IntegrationTestCase(unittest.TestCase, ABC):
        if start_services:
            @pytest.fixture(autouse=True)
            def inject_docker_services(self, docker_ip, docker_services):
                self.docker_ip = docker_ip
                self.docker_services = docker_services
        
        def setUp(self):
            containers = docker.from_env().containers
            for name, listen in listen_services.items():
                if listen:
                    container, = containers.list(filters={"label": f"com.docker.compose.service={name}"})
                    def check():
                        container.reload()
                        return container.health == 'healthy'
                    wait_until_true(check, 60, 1, msg=name)
            self.dbc = MongoClient(host=self.docker_ip, port=self.docker_services.port_for("mongo", 27017)) if start_services else MongoClient()
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