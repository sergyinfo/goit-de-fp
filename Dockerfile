FROM bitnami/spark:3

USER root

RUN pip install --no-cache-dir mysql-connector-python

USER 1001