import pytest, os, pathlib

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(pathlib.Path(str(pytestconfig.rootdir)) / 'docker-compose.yml')
