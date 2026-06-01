FROM flink:1.19-scala_2.12-java17

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*


RUN pip3 install --no-cache-dir \
    apache-flink==1.19.0 \
    protobuf \
    google \
    kafka-python

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

RUN mkdir -p /opt/flink/jobs

ENV PYTHONPATH "${PYTHONPATH}:/opt/flink/jobs"

WORKDIR /opt/flink

COPY jobs /opt/flink/jobs/

RUN chmod -R 755 /opt/flink/jobs