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
    install_requires=["sqlalchemy==0.8.2",
                      "mako==0.8.0"],
    ext_modules=[dispatcher_ext]
)
