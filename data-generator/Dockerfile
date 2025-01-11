# Base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy only requirements first to leverage Docker caching
COPY requirements.txt /app/

# Install required Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application code into the container
COPY . /app

# Define environment variables
ENV INGESTION_SVC_WS="ws://localhost:1111"

# Command to run the Python application
CMD ["python", "generator.py"]