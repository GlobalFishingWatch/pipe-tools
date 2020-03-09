#!/usr/bin/env python

"""
Setup script for pipe-tools
"""

import codecs
import os
import sys

from setuptools import find_packages
from setuptools import setup
from pipe_tools.beam.requirements import requirements as DATAFLOW_PINNED_DEPENDENCIES

DEPENDENCIES = [
    "newlinejson",
    "nose",
    "pytest",
    "python-dateutil",
    "pytz",
    "udatetime",
    "ujson==1.35",
    "six>=1.12"
]

SCRIPTS = [
    'bin/pipe-tools-utils',
    'bin/xdaterange',
]


with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()


version = None
author = None
email = None
source = None
with open(os.path.join('pipe_tools', '__init__.py')) as f:
    for line in f:
        if line.strip().startswith('__version__'):
            version = line.split('=')[1].strip().replace(
                '"', '').replace("'", '')
        elif line.strip().startswith('__author__'):
            author = line.split('=')[1].strip().replace(
                '"', '').replace("'", '')
        elif line.strip().startswith('__email__'):
            email = line.split('=')[1].strip().replace(
                '"', '').replace("'", '')
        elif line.strip().startswith('__source__'):
            source = line.split('=')[1].strip().replace(
                '"', '').replace("'", '')
        elif None not in (version, author, email, source):
            break


setup(
    author=author,
    author_email=email,
    description="A python utility library for apache beam and bigquery",
    include_package_data=True,
    install_requires=DEPENDENCIES + DATAFLOW_PINNED_DEPENDENCIES,
    keywords='AIS GIS remote sensing',
    license="Apache 2.0",
    long_description=readme,
    name='pipe-tools',
    packages=find_packages(exclude=['test*.*', 'tests']),
    url=source,
    version=version,
    zip_safe=True,
    scripts=SCRIPTS
)
