FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark


USER root
COPY run.sh /run.sh
RUN chmod a+x /run.sh



CMD ["/run.sh"]