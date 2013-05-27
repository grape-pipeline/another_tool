from distribute_setup import use_setuptools
use_setuptools()
import jip
from setuptools import setup

setup(
    name='another_tool',
    version=jip.__version__,
    description='JIP pipeline library',
    author='Thasso Griebel',
    author_email='thasso.griebel@gmail.com',
    url='',
    license="BSD",
    long_description='''This is yet another pipeline library''',
    packages=['jip'],
)
