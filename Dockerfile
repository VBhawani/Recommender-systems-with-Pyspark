FROM python:3.9-slim

WORKDIR /app

COPY . /app/

RUN pip install requirements.txt

# Install Java (JDK 21) and other necessary libraries
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark (version 3.5.4) with Hadoop 3.3.6
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.3.tgz && \
    tar -xzf spark-3.5.4-bin-hadoop3.3.tgz && \
    mv spark-3.5.4-bin-hadoop3.3 /opt/spark && \
    rm spark-3.5.4-bin-hadoop3.3.tgz

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PYSPARK_DRIVER_PYTHON=python
ENV PYSPARK_PYTHON=python3

# Expose the port the app runs on (Streamlit default port 8501)
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "app/app.py"]



