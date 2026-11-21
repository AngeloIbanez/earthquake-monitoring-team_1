FROM apache/airflow:2.10.4

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir \
    "pandas>=2.2.0" \
    "polars>=1.0.0" \
    "pyarrow>=14.0.0" \
    "requests>=2.30.0" \
    "pyyaml>=6.0.0" \
    "pyspark>=3.5.0" \
    "dash>=2.14.0" \
    "plotly>=5.18.0"
