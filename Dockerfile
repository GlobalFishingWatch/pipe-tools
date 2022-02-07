FROM python:3.8

RUN mkdir -p /opt/project
WORKDIR /opt/project

COPY . /opt/project
RUN pip install -e .[test]
