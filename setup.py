from setuptools import setup, Extension

import os
import subprocess


try:
    KALDI_HOME = os.environ['KALDI_HOME']
    with open(os.path.join('trunkindexer', '_config.py'), 'w') as fh:
        fh.write('KALDI_HOME = """{}"""'.format(KALDI_HOME))
        fh.write("\n")
except KeyError:
    raise RuntimeError(
        'The environment variable KALDI_HOME must be set to where '
        'kaldi is located'
    )
from setuptools.command.build_py import build_py as _build_py


class build_py(_build_py):
    def run(self):

        # build vendored kenlm so we can package the lmplz tool
        p = subprocess.Popen(
            'cmake . && make',
            shell=True,
            cwd=os.path.join(
                os.path.dirname(__file__),
                'trunkindexer/vendor/kenlm'
            )
        )
        p.wait()
        return super().run()


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

    packages=['trunkindexer'],
    setup_requires=[
        'numpy'  # sequitur-g2p uses numpy in setup.py :(
    ],
    install_requires=[
        'colorama',
        'elasticsearch',
        'fiona',
        'lark-parser',
        'numpy',
        'pytz',
        'shapely',
        'tzlocal',
        'sequitur==1.0a1',
    ],
    dependency_links=[
        'https://github.com/cnelson/sequitur-g2p/archive/master.zip'
        '#egg=sequitur-1.0a1'
    ],

    test_suite='trunkindexer.tests',

    entry_points={
        'console_scripts': [
            'trunkindexer = trunkindexer.cli:entrypoint'
        ]
    },
    ext_modules=[kaldi],
    cmdclass={'build_py': build_py},
    package_data={
        'trunkindexer':
            [
                os.path.join(x[0], y).replace('trunkindexer/', '')
                for x in os.walk('trunkindexer/fixtures') for y in x[2]
            ] + ['vendor/kenlm/bin/lmplz', 'data/aspire/*']
    },

)
