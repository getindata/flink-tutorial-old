ARG FLINK_VERSION
FROM flink:$FLINK_VERSION-scala_2.11-java8

RUN mkdir -p /opt/sql-client/lib/
RUN mkdir -p /opt/sql-client/conf/

# FIXME: $FLINK_VERSION does not work
# Download connector libraries
RUN wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.13.2/flink-sql-connector-kafka_2.11-1.13.2.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.13.2/flink-sql-avro-confluent-registry-1.13.2.jar; \
    wget -P /opt/sql-client/lib/ https://repo1.maven.org/maven2/com/alibaba/ververica/flink-connector-mysql-cdc/1.4.0/flink-connector-mysql-cdc-1.4.0.jar;

COPY sql-client.sh /opt/sql-client/
COPY sql-client-conf.yaml /opt/sql-client/conf/

WORKDIR /opt/sql-client/
ENV SQL_CLIENT_HOME /opt/sql-client/

COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
