FROM python:2.7

# Airflow GPL dependency
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes

RUN mkdir -p /opt/project
WORKDIR /opt/project

COPY . /opt/project
RUN pip install -e .
