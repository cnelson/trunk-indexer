from setuptools import setup, find_packages, Extension

import os

try:
    KALDI_HOME = os.environ['KALDI_HOME']
except KeyError:
    print(
        'The environment variable KALDI_HOME must be set to where '
        'kaldi is located'
    )
    raise SystemExit


kaldi = Extension(
        'trunkindexer.kaldi',
        sources=['trunkindexer/kaldi.cc'],
        include_dirs=[
            os.path.join(KALDI_HOME, 'src'),
            os.path.join(KALDI_HOME, 'tools/openfst/include')
        ],
        extra_objects=[
            os.path.join(KALDI_HOME, 'src/online2/libkaldi-online2.so')
        ],
        runtime_library_dirs=[
            os.path.join(KALDI_HOME, 'src/lib')
        ],
        extra_compile_args=['-w']  # fuuuu openfst
)

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
    },
    ext_modules=[kaldi]
)
