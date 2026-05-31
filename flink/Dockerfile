FROM flink:1.19-scala_2.12-java17

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.19.0

RUN wget -P /opt/flink/lib \
https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

WORKDIR /opt/flink

COPY jobs /opt/flink/jobs