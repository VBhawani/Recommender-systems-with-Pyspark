# Use the official Apache Spark PySpark image
FROM apache/spark-py:v3.4.0

# Set the working directory in the container
WORKDIR /app

# Copy your application files into the container
COPY . /app/

# Install any additional Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port Streamlit runs on (default is 8501)
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "app/app.py"]
