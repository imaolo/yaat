# from __future__ import annotations

from yaat.mongo import MongoDoc
from pymongo import MongoClient
from abc import ABC
import pytest, unittest, docker, time


class IntegrationTestCase(unittest.TestCase, ABC):

    def __init_subclass__(cls, *, wait_mongo=True, wait_yaat=True, **kwargs):
        super().__init_subclass__(**kwargs)
        # You can assign the parameter to the class attribute or do any initialization logic here.
        cls.wait_mongo = wait_mongo
        cls.wait_yaat = wait_yaat

    @pytest.fixture(autouse=True)
    def inject_docker_services(self, docker_ip, docker_services):
        self.docker_ip = docker_ip
        self.docker_services = docker_services

    def setUp(self):
        if self.wait_mongo:
            self.dbc = MongoClient(host=self.docker_ip, port=self.docker_services.port_for("mongo", 27017))
            self.docker_services.wait_until_responsive(
                timeout=30.0,
                pause=0.1,
                check=lambda: self.dbc['admin'].command('ping')
            )

        if self.wait_yaat:
            container, = docker.from_env().containers.list(filters={"label": f"com.docker.compose.service=yaat"})
            def check_mongo():
                container.reload()
                return container.health == 'healthy'
            self.docker_services.wait_until_responsive(
                timeout=30.0,
                pause=0.1,
                check=check_mongo
            )
            # TODO - needed?
            time.sleep(0.25)

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
    t1: type[Doc1]