FROM python:3.11-slim-bookworm

ARG PIG_VERSION=0.18.0
ARG HADOOP_VERSION=3.3.6
ARG HIVE_VERSION=4.1.0

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PIG_HOME=/opt/pig-${PIG_VERSION}
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV HADOOP_BIN=${HADOOP_HOME}/bin/hadoop
ENV HADOOP_STREAMING_JAR=${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-${HADOOP_VERSION}.jar
ENV HIVE_BIN=${HIVE_HOME}/bin/hive
ENV HIVE_BEELINE_BIN=${HIVE_HOME}/bin/beeline
ENV HIVE_JDBC_URL=jdbc:hive2://
ENV PIG_CLASSPATH=/usr/share/java/commons-text.jar:/usr/share/java/commons-compress.jar:/usr/share/java/commons-lang3.jar
ENV PATH=${JAVA_HOME}/bin:${PIG_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${HIVE_HOME}/bin:${PATH}

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        libcommons-compress-java \
        libcommons-lang3-java \
        libcommons-text-java \
        openjdk-17-jdk-headless \
        procps \
        tar \
    && rm -rf /var/lib/apt/lists/*

# 1. PIG
RUN curl -fL --retry 5 --connect-timeout 30 --max-time 600 \
        "https://downloads.apache.org/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz" \
        -o /tmp/pig.tar.gz || \
    curl -fL --retry 5 --connect-timeout 30 --max-time 600 \
        "https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz" \
        -o /tmp/pig.tar.gz \
    && tar -xzf /tmp/pig.tar.gz -C /opt \
    && rm /tmp/pig.tar.gz

# 2. HADOOP
RUN curl -fL --retry 5 --connect-timeout 30 --max-time 900 \
        "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        -o /tmp/hadoop.tar.gz || \
    curl -fL --retry 5 --connect-timeout 30 --max-time 900 \
        "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        -o /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && rm /tmp/hadoop.tar.gz

# 3. HIVE
RUN curl -fL --retry 5 --connect-timeout 30 --max-time 1200 \
        "https://downloads.apache.org/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" \
        -o /tmp/hive.tar.gz || \
    curl -fL --retry 5 --connect-timeout 30 --max-time 1200 \
        "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" \
        -o /tmp/hive.tar.gz \
    && tar -xzf /tmp/hive.tar.gz -C /opt \
    && rm /tmp/hive.tar.gz

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash"]
