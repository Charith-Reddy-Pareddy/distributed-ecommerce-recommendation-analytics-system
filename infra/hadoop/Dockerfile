# Hadoop is plain Java -- the JARs are architecture-neutral. The official
# apache/hadoop Docker image is amd64-only (built on an amd64 base OS), which
# means Rosetta emulation on Apple Silicon: a real cost for a JVM service in
# a tight memory budget. Building on eclipse-temurin (which *does* publish a
# native arm64 JRE) and dropping in the stock Hadoop binary tarball gives
# native performance on both amd64 and arm64 hosts.
FROM eclipse-temurin:11-jre

ARG HADOOP_VERSION=3.4.2

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | tar -xz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root

COPY config/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY config/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY entrypoint-namenode.sh entrypoint-datanode.sh /

RUN chmod +x /entrypoint-namenode.sh /entrypoint-datanode.sh
