from pymongo import MongoClient
from yaat.yaat import TopMoversScraper
from tests.common import docker_compose_file, IntegrationTestCase

class TestYaat(IntegrationTestCase):

    def test_sample(self):
        coll = MongoClient()[TopMoversScraper.dbname][TopMoversScraper.collname]
        self.assertGreater(coll.count_documents({}), 1)
