from __future__ import annotations
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
                wait_until_true(check, timeout=60, pause=0.1, msg=name)