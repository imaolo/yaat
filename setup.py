import setuptools
setuptools.setup(
    name='yaat',version='0.1',
    packages=setuptools.find_packages(),
    install_requires = ['pandas', 'torch', 'transformers', 'gdown']
)