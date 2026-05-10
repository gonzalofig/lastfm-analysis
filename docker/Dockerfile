FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless tini \
    && rm -rf /var/lib/apt/lists/*

COPY docker/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /workspace

EXPOSE 8888 4040

ENTRYPOINT ["tini", "--"]
CMD ["bash"]
