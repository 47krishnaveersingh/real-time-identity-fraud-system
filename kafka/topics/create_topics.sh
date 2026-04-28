#!/bin/bash

echo "Creating Kafka topics..."

kafka-topics --create --topic login_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic transaction_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic device_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

echo "Topics created successfully!"