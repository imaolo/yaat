from pymongo import MongoClient
from yaat.yaat import TopMoversScalper
from tests.common import docker_compose_file, IntegrationTestCase
import unittest, pytest, time

class TestYaat(IntegrationTestCase):

    def test_sample(self):
        coll = MongoClient()[TopMoversScalper.dbname][TopMoversScalper.collname]
        self.assertGreater(coll.count_documents({}), 1)
