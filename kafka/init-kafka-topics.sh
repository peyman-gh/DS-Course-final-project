#!/bin/sh

KT="/opt/bitnami/kafka/bin/kafka-topics.sh"

echo "Waiting for kafka..."
"$KT" --bootstrap-server localhost:9092 --list

echo "Creating kafka topics"
"$KT" --bootstrap-server localhost:9092 --create --if-not-exists --topic market_data --replication-factor 1 --partitions 1
"$KT" --bootstrap-server localhost:9092 --create --if-not-exists --topic signals --replication-factor 1 --partitions 1

echo "Successfully created the following topics:"
"$KT" --bootstrap-server localhost:9092 --list