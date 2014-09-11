#!/usr/bin/env python
from setuptools import setup

setup(
    name="keyvalue",
    version="0.2.1",
    author="Yuri Baburov",
    author_email="burchik@gmail.com",
    description="simple pythonic key-value DB access, caching and pipes",
    long_description=open("README.md").read(),
    license="MIT",
    url="http://github.com/buriy/python-keyvalue",
    packages=['keyvalue', 'keyvalue.leveldb', 'keyvalue.bloom', 'keyvalue.redis'],
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        ],
)
