from distribute_setup import use_setuptools
use_setuptools()

from setuptools import setup

__VERSION_MAJOR = "1"
__VERSION_MINOR = "0"
__VERSION__ = "%s.%s" % (__VERSION_MAJOR, __VERSION_MINOR)

if __name__ == "__main__":
    setup(
            name='rnaseq-pipeline',
            version=__VERSION__,
            description='CNAG RNASeq pipeline and utilities',
            author='Thasso Griebel',
            author_email='thasso.griebel@gmail.com',
            url='',
            license="GNU General Public License (GPL)",
            long_description='''Package containing various utilities for the rnaseq pipeline''',
            packages=['rna'],
            install_requires=['argparse', 'requests'],
            test_suite='nose.collector',
            entry_points={
                'console_scripts': [
                    'rna = rna.commands:main',
                ]
            }
    )
