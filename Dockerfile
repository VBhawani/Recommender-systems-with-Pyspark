# Apache Spark PySpark image with version 3.4.0
FROM apache/spark-py:v3.4.0

# Switches the user to root so you can install system packages.
USER root
RUN apt-get update && apt-get install -y sudo

# Set working directory in the container
WORKDIR /app

# Copy your application files into the container
COPY . /app/

# Install any additional Python dependencies with sudo
RUN sudo pip install --no-cache-dir -r requirements.txt

# Expose the port Streamlit runs on (default is 8501)
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "app/app.py"]
