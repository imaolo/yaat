import pytest, os, pathlib

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(pathlib.Path(str(pytestconfig.rootdir)) / 'docker-compose.yml')

@pytest.fixture(scope="class")
def init_class_docker(request, docker_services, docker_ip):
    request.cls.docker_ip = docker_ip
    request.cls.listen_services()
    yield