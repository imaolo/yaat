from yaat.mongo import MongoDoc
from pymongo import MongoClient
import pytest, unittest, docker

class IntegrationTestCase(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def inject_docker_services(self, docker_ip, docker_services):
        self.docker_ip = docker_ip
        self.docker_services = docker_services

    def setUp(self):
        # create mongoclient and wait on connection
        self.dbc = MongoClient(host=self.docker_ip, port=self.docker_services.port_for("mongo", 27017))
        self.docker_services.wait_until_responsive(
            timeout=30.0,
            pause=0.1,
            check=lambda: self.dbc['admin'].command('ping')
        )

        # wait on yaat
        container, = docker.from_env().containers.list(filters={"label": f"com.docker.compose.service=yaat"})
        def check_mongo():
            container.reload()
            return container.health == 'healthy'
        self.docker_services.wait_until_responsive(
            timeout=30.0,
            pause=0.1,
            check=check_mongo
        )

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
