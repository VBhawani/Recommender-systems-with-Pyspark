# Use an OpenJDK base image with JDK 21
FROM openjdk:21-slim

# Install Python 3.9 and other necessary packages
RUN apt-get update && apt-get install -y \
    python3.9 \
    python3-pip \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install other dependencies from requirements.txt
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Set the working directory in the container
WORKDIR /app

# Copy your application code into the container
COPY . /app

# Install Spark 3.5.4 (with Hadoop 3.3.6)
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.3.tgz && \
    tar -xzf spark-3.5.4-bin-hadoop3.3.tgz && \
    mv spark-3.5.4-bin-hadoop3.3 /opt/spark && \
    rm spark-3.5.4-bin-hadoop3.3.tgz

# Set environment variables for Spark and Java
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYSPARK_PYTHON=python3

# Expose the port that Streamlit runs on
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "app/app.py"]
