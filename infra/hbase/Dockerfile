# Same reasoning as infra/hadoop: HBase is plain Java, so its distribution
# is architecture-neutral -- there's no official Apache HBase Docker image
# at all (let alone an arm64 one), so building on eclipse-temurin (native
# arm64 JRE) with the stock binary tarball gets native performance without
# depending on a third-party image.
FROM eclipse-temurin:11-jre

ARG HBASE_VERSION=2.5.11

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz" \
    | tar -xz -C /opt \
    && mv /opt/hbase-${HBASE_VERSION} /opt/hbase

ENV HBASE_HOME=/opt/hbase
ENV PATH=$HBASE_HOME/bin:$PATH
# Using the external zookeeper container (below), not HBase's own
# built-in/managed ZooKeeper.
ENV HBASE_MANAGES_ZK=false

COPY config/hbase-site.xml $HBASE_HOME/conf/hbase-site.xml
COPY entrypoint-master.sh entrypoint-regionserver.sh entrypoint-rest.sh /

RUN chmod +x /entrypoint-master.sh /entrypoint-regionserver.sh /entrypoint-rest.sh
