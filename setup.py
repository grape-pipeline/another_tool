from distribute_setup import use_setuptools
use_setuptools()
import jip
from setuptools import setup, Extension


dispatcher_ext = Extension('jip.dispatcher',
                           ['jip/dispatcher/jip_binding.c',
                            'jip/dispatcher/jip_dispatcher.c'])

setup(
    name='jip',
    version=jip.__version__,
    description='JIP pipeline library',
    author='Thasso Griebel',
    author_email='thasso.griebel@gmail.com',
    url='',
    license="BSD",
    long_description='''This is yet another pipeline library''',
    packages=['jip'],
    ext_modules=[dispatcher_ext]
)
