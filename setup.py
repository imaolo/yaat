from setuptools import setup, find_packages
setup(
    name="yaat",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "openai-agents>=0.2.0",
        "python-dotenv>=1.0.1"
    ],
    python_requires=">=3.12",
)
