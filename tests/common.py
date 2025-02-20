from yaat.mongo import MongoDoc
from pathlib import Path
from pymongo import MongoClient
import pytest, os, unittest, time

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(Path(str(pytestconfig.rootdir)) / 'docker-compose.yml')

class IntegrationTestCase(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def inject_docker_services(self, docker_ip, docker_services):
        self.docker_ip = docker_ip
        self.docker_services = docker_services

    def setUp(self):
        port = self.docker_services.port_for("mongo", 27017)
        url = "mongodb://{}:{}".format(self.docker_ip, port)
        dbc = MongoClient(url)
        self.docker_services.wait_until_responsive(
            timeout=30.0,
            pause=0.1,
            check=lambda: dbc['admin'].command('ping')
        )
        time.sleep(10)

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
