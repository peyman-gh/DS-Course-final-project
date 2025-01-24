FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DATABASE_URI=postgresql://postgres:postgres@localhost:5432/postgres
ENV REDIS_HOST=localhost
ENV REDIS_PORT=6379
ENV KAFKA_TOPIC=stock
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV KAFKA_CONSUMER_GROUP=analyze

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose ports
EXPOSE 9092
EXPOSE 6379
EXPOSE 5432

# Command to run the service
CMD ["python", "aggregation_service.py"]
