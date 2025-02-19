from pathlib import Path
from pymongo import MongoClient
from yaat.yaat import TopMoversScalper
import unittest, pytest, os, time

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(Path(str(pytestconfig.rootdir)) / 'docker-compose.yml')

class TestApp(unittest.TestCase):

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

    def test_sample(self):
        coll = MongoClient()[TopMoversScalper.dbname][TopMoversScalper.collname]
        assert coll.count_documents({}) > 1

