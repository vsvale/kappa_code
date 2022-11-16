# https://quay.io/repository/strimzi/kafka?tab=tags
FROM quay.io/strimzi/kafka:latest-kafka-3.2.3

# AS root
USER root:root

# create dirs
RUN mkdir -p /opt/kafka/plugins/cdc-mysql
RUN mkdir -p /opt/kafka/plugins/cdc-postgres
RUN mkdir -p /opt/kafka/plugins/cdc-sqlsserver
RUN mkdir -p /opt/kafka/plugins/jdbc
RUN mkdir -p /opt/kafka/plugins/mongodb
RUN mkdir -p /opt/kafka/plugins/s3-source
RUN mkdir -p /opt/kafka/plugins/s3-sync
RUN mkdir -p /opt/kafka/plugins/yugabytedb
RUN mkdir -p /opt/kafka/plugins/elastic-src
RUN mkdir -p /opt/kafka/plugins/elastic-sync

# copy jars
COPY ./jars/cdc-mysql/ /opt/kafka/plugins/cdc-mysql
COPY ./jars/cdc-postgres/ /opt/kafka/plugins/cdc-postgres
COPY ./jars/cdc-sqlsserver/ /opt/kafka/plugins/cdc-sqlsserver
COPY ./jars/jdbc/ /opt/kafka/plugins/jdbc
COPY ./jars/mongodb /opt/kafka/plugins/mongodb
COPY ./jars/s3-source/ /opt/kafka/plugins/s3-source
COPY ./jars/s3-sync/ /opt/kafka/plugins/s3-sync
COPY ./jars/yugabytedb/ /opt/kafka/plugins/yugabytedb
COPY ./jars/elastic-src/ /opt/kafka/plugins/elastic-src
COPY ./jars/elastic-sync/ /opt/kafka/plugins/elastic-sync

# copy libs
COPY ./jars/libs/ /opt/kafka/libs/

# user
USER 1001