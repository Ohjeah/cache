import os
import sys
from os.path import dirname

from setuptools import setup

if sys.argv[-1] == "publish":
    os.system("python setup.py sdist bdist_wheel upload")
    sys.exit()

required = [
    "dill",
]

setup(
    name='cache',
    version='0.0.0',
    description='cacheing',
    long_description="",
    author='Markus Quade',
    author_email='info@markusqua.de',
    url='https://github.com/ohjeah/cache',
    packages=['cache'],
    install_requires=required,
    license='MIT',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
