# Use the official Bitnami Kafka image as base
FROM docker.io/bitnami/kafka

# Set environment variables for Kafka configuration
ENV KAFKA_CFG_NODE_ID=0 \
    KAFKA_CFG_PROCESS_ROLES=controller,broker \
    KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093 \
    KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094 \
    KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094 \
    KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true \
    ALLOW_PLAINTEXT_LISTENER=yes

# Copy initialization scripts
COPY run-init-kafka-topics.sh /docker-entrypoint-initdb.d/
COPY init-kafka-topics.sh /

# Expose the Kafka port
EXPOSE 9092 9093 9094



