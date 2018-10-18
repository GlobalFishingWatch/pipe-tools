#!/usr/bin/env python

"""
Setup script for pipe-tools
"""

import codecs
import os

from setuptools import find_packages
from setuptools import setup


DEPENDENCIES = [
    "pytest",
    "nose",
    "ujson",
    "pytz",
    "udatetime",
    "python-dateutil",
    "newlinejson",
    "pandas-gbq",
]

AIRFLOW_DEPENDENCIES = [
    "google-api-python-client"
]

# We pin dependencies of airflow and dataflow because we've had instances
# where the latest libraries do not play well together. Pinning is accomplished by 
# creating an empty environment and running:
'''
pip install apache-beam[gcp]==2.6.0
pip install apache-airflow==1.9.0
'''
# Then collecting the dependencies using `pip freeze`
PINNED_DEPENDENCIES = """
alembic==0.8.10
apache-airflow==1.9.0
apache-beam==2.6.0
avro==1.8.2
bleach==2.1.2
cachetools==2.1.0
certifi==2018.10.15
chardet==3.0.4
Click==7.0
configparser==3.5.0
crcmod==1.7
croniter==0.3.25
dill==0.2.8.2
docopt==0.6.2
docutils==0.14
enum34==1.1.6
fasteners==0.14.1
Flask==0.11.1
Flask-Admin==1.4.1
Flask-Cache==0.13.1
Flask-Login==0.2.11
flask-swagger==0.2.13
Flask-WTF==0.14
funcsigs==1.0.0
future==0.16.0
futures==3.2.0
gapic-google-cloud-pubsub-v1==0.15.4
gitdb2==2.0.5
GitPython==2.1.11
google-apitools==0.5.20
google-auth==1.5.1
google-auth-httplib2==0.0.3
google-cloud-bigquery==0.25.0
google-cloud-core==0.25.0
google-cloud-pubsub==0.26.0
google-gax==0.15.16
googleapis-common-protos==1.5.3
googledatastore==7.0.1
grpc-google-iam-v1==0.11.4
grpcio==1.15.0
gunicorn==19.9.0
hdfs==2.1.0
html5lib==1.0.1
httplib2==0.11.3
idna==2.7
itsdangerous==0.24
Jinja2==2.8.1
lockfile==0.12.2
lxml==3.8.0
Mako==1.0.7
Markdown==2.6.11
MarkupSafe==1.0
mock==2.0.0
monotonic==1.5
numpy==1.15.2
oauth2client==4.1.3
ordereddict==1.1
pandas==0.23.4
pbr==5.0.0
ply==3.8
proto-google-cloud-datastore-v1==0.90.4
proto-google-cloud-pubsub-v1==0.15.4
protobuf==3.6.1
psutil==4.4.2
pyasn1==0.4.4
pyasn1-modules==0.2.2
pydot==1.2.4
Pygments==2.2.0
pyparsing==2.2.2
python-daemon==2.1.2
python-dateutil==2.7.3
python-editor==1.0.3
python-nvd3==0.14.2
python-slugify==1.1.4
pytz==2018.4
PyVCF==0.6.8
PyYAML==3.13
requests==2.19.1
rsa==4.0
setproctitle==1.1.10
six==1.11.0
smmap2==2.0.5
SQLAlchemy==1.2.12
tabulate==0.7.7
thrift==0.11.0
typing==3.6.6
Unidecode==1.0.22
urllib3==1.23
webencodings==0.5.1
Werkzeug==0.14.1
WTForms==2.2.1
zope.deprecation==4.3.0
""".strip().split()

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
            version = line.split('=')[1].strip().replace('"', '').replace("'", '')
        elif line.strip().startswith('__author__'):
            author = line.split('=')[1].strip().replace('"', '').replace("'", '')
        elif line.strip().startswith('__email__'):
            email = line.split('=')[1].strip().replace('"', '').replace("'", '')
        elif line.strip().startswith('__source__'):
            source = line.split('=')[1].strip().replace('"', '').replace("'", '')
        elif None not in (version, author, email, source):
            break


setup(
    author=author,
    author_email=email,
    description="A python utility library for apache beam and bigquery",
    include_package_data=True,
    install_requires=DEPENDENCIES + AIRFLOW_DEPENDENCIES + PINNED_DEPENDENCIES,
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



