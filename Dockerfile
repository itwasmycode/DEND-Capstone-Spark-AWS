FROM python:3.7-slim

ENV SPARK_VERSION 2.4.5
ENV HADOOP_VERSION 2.7

RUN apt-get update && apt-get install -y curl openjdk-11-jre-headless

RUN curl -sL --retry 3 \
  "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" \
  | tar xz -C /usr/local/

ENV SPARK_HOME /usr/local/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV PATH $PATH:$SPARK_HOME/bin

RUN pip install --upgrade pip

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY simple-pyspark-app.py /app/

CMD ["spark-submit", "--master", "local[*]", "/app/simple-pyspark-app.py"]
