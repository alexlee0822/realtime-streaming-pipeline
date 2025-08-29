FROM apache/spark:3.5.1

USER root
RUN mkdir -p /opt/spark-jobs /opt/delta /opt/checkpoints \
    && chown -R spark:spark /opt/spark-jobs /opt/delta /opt/checkpoints
USER spark

WORKDIR /opt/spark-jobs
