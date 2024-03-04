import setuptools
setuptools.setup(
    name='yaat', version='0.1',
    packages=setuptools.find_packages(),
    install_requires = [
        'torch', 'dropbox',
        'Informer2020 @ git+https://github.com/imaolo/Informer2020.git#egg=Informer2020']
)