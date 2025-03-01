from setuptools import setup, find_packages # type: ignore

setup(
    name='yaat',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pymongo',
        'requests',
        'apscheduler',
        'httpie',
        'dotenv',
        'mongoengine',
        'fastapi',
        'uvicorn',
        'marshmallow_mongoengine',
        'marshmallow_jsonschema'
    ],
    extras_require={
        'test': [
            'pytest-docker',
            'pytest',
            'docker'
        ]
    }
)