FROM python@sha256:3079b0d54fd139626b117f01ce61fb3e84e37b73c5334d80b47cdd196a0e5036

RUN mkdir -p /opt/project
WORKDIR /opt/project

COPY . /opt/project
RUN pip install -e .
