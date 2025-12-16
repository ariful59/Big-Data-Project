FROM bitnami/spark:latest

USER root

RUN apt-get update \
    && apt-get install -y netcat-traditional iputils-ping nano \
    && pip install pandas \
    && rm -rf /var/lib/apt/lists/*

# Optionally set netcat-traditional as default `nc`
RUN update-alternatives --set nc /bin/nc.traditional || true

# Create a non-root user and prepare checkpoint dir
RUN useradd -u 1001 -ms /bin/bash sparkuser \
    && mkdir -p /tmp/spark-checkpoint \
    && chmod -R 777 /tmp/spark-checkpoint

USER sparkuser
