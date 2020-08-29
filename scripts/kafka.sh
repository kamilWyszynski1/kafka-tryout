#!/bin/bash

# The following commands will start a container with Kafka and Zookeeper running on mapped ports 2181 (Zookeeper) and 9092 (Kafka).

# Setting ADVERTISED_HOST to localhost, 127.0.0.1, or 0.0.0.0 will work great
# only if Producers and Consumers are started within the kafka container itself
docker run -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 --name kafka -h kafka spotify/kafka
