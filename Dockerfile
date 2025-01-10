# Apache Spark PySpark image with version 3.4.0
FROM apache/spark-py:v3.4.0

# Switches user to root 
USER root
RUN apt-get update && apt-get install -y sudo

# Set working directory in container
WORKDIR /app

# Copy your application files into container
COPY . /app/

# Install any additional Python dependencies with sudo
RUN sudo pip install --no-cache-dir -r requirements.txt

# Expose the port Streamlit runs on (default is 8501)
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "app/app.py"]
