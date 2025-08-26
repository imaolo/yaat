from tests.common import IntegrationTestCase

class TestYaat(IntegrationTestCase, services={'yaat': False}):
    @classmethod
    def setUpClass(cls):
        pass

    def test_simple(self):
        pass
