# Dockerfile for PyFlink + Kafka & JSON connectors
FROM flink:1.18.1-scala_2.12

RUN apt-get update \
 && apt-get install -y python3 python3-pip curl \
 && pip3 install --upgrade pip apache-flink \
 && mkdir -p /opt/flink/lib \
# download the Flink SQL Kafka connector (for Flink 1.18.x)
&& curl -fsSL \
     https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar \
     -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
 # JSON format support for Flink SQL & Table API
 && curl -fsSL \
    https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.18.1/flink-json-1.18.1.jar \
    -o /opt/flink/lib/flink-json-1.18.1.jar
