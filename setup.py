from setuptools import setup, find_packages

setup(
    name='trunkindexer',

    version='0.0.1',

    description='STT and visualizations for trunk-recorder',

    url='https://github.com/cnelson/trunk-indexer',

    author='Chris Nelson',
    author_email='cnelson@cnelson.org',

    classifiers=[
        'License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication',
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3',
    ],

    keywords='trunk-recorder ops25 p25 gnuradio',

    packages=find_packages(),

    install_requires=[
        'colorama',
        'elasticsearch',
        'fiona',
        'lark-parser',
        "pytz",
        'shapely',
        "tzlocal"
    ],

    test_suite='trunkindexer.tests',

    entry_points={
        'console_scripts': [
            'trunkindexer = trunkindexer.cli:entrypoint'
        ]
    }
)
