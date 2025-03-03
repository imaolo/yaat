from setuptools import setup, find_packages # type: ignore

setup(
    name='yaat',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'requests',
        'apscheduler',
        'httpie',
        'dotenv',
        'beanie',
        'fastapi',
        'uvicorn',
    ],
    extras_require={
        'test': [
            'pytest-docker',
            'pytest',
            'docker'
        ]
    }
)