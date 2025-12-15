FROM bitnami/spark:latest

USER root

RUN apt-get update \
    && apt-get install -y netcat-traditional iputils-ping nano \
    && rm -rf /var/lib/apt/lists/*

# optionally set netcat‑traditional as default `nc`
RUN update-alternatives --set nc /bin/nc.traditional || true

RUN useradd -u 1001 -ms /bin/bash sparkuser \
    && mkdir -p /tmp/spark-checkpoint \
    && chmod -R 777 /tmp/spark-checkpoint

USER sparkuser