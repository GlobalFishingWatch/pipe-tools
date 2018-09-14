FROM python:2.7

RUN mkdir -p /opt/project
WORKDIR /opt/project

COPY . /opt/project
RUN pip install -e .
