import setuptools
setuptools.setup(
    name='yaat', version='0.1',
    packages=setuptools.find_packages(),
    install_requires = ['Informer2020 @ git+https://github.com/imaolo/Informer2020.git@yaat#egg=Informer2020',
                        'chronos @ git+https://github.com/amazon-science/chronos-forecasting.git',
                        'pymongo', 'pandas', 'polygon-api-client', 'requests', 'tqdm', 'gdown', 'python-dotenv'],
    extras_require={'testing': ['pytest']},
)