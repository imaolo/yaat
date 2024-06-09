import setuptools
setuptools.setup(
    name='yaat', version='0.1',
    packages=setuptools.find_packages(),
    install_requires = ['python-dotenv', 'tabulate', 'pandas', 'pymongo', 'requests', 'pandas_market_calendars'],
    # 'torch'
    # 'Informer2020 @ git+https://github.com/imaolo/Informer2020.git#egg=Informer2020'
    extras_require={'testing': ['pytest']},
)