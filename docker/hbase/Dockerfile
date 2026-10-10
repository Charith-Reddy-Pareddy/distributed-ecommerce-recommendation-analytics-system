FROM eclipse-temurin:17-jre-jammy

ARG HBASE_VERSION=2.6.7
ENV HBASE_HOME=/opt/hbase
ENV PATH="${HBASE_HOME}/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tar \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system hbase \
    && useradd --system --gid hbase --home-dir /data/hbase hbase

WORKDIR /opt
RUN curl -fsSLO "https://downloads.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz" \
    && curl -fsSLO "https://downloads.apache.org/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz.sha512" \
    && expected="$(sed 's/^[^:]*: *//' "hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz.sha512" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')" \
    && actual="$(sha512sum "hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz" | cut -d ' ' -f 1)" \
    && test "${#expected}" -eq 128 \
    && test "$expected" = "$actual" \
    && tar -xzf "hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz" \
    && ln -s "/opt/hbase-${HBASE_VERSION}-hadoop3" "${HBASE_HOME}" \
    && rm "hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz" "hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz.sha512"

COPY hbase-site.xml "${HBASE_HOME}/conf/hbase-site.xml"
COPY start-hbase.sh /usr/local/bin/start-hbase.sh
RUN chmod +x /usr/local/bin/start-hbase.sh \
    && mkdir -p /data/hbase \
    && chown -R hbase:hbase /opt/hbase-${HBASE_VERSION}-hadoop3 /data/hbase

USER hbase
EXPOSE 8080
VOLUME ["/data/hbase"]
CMD ["/usr/local/bin/start-hbase.sh"]
