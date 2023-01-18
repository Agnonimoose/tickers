from setuptools import setup

setup(
    name='stockTickers',
    packages=['StockTickers'],
    include_package_data=True,
    install_requires=[
        'requests',
        'bs4',
        'pandas',
        'aiohttp'
    ],
)