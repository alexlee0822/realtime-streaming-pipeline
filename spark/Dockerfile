FROM bitnami/spark:3.5.1

USER root
RUN mkdir -p /opt/spark-jobs /opt/delta /opt/checkpoints \
    && chown -R 1001:1001 /opt/spark-jobs /opt/delta /opt/checkpoints
USER 1001

WORKDIR /opt/spark-jobs
