#!/bin/bash
# This command will run an unnamed instance of spotify/kafka linked to the kafka service,
# start a producer, and wait for newline-delimited input until you quit (which destroys the container):
docker run -it --rm --link kafka spotify/kafka /opt/kafka_2.11-0.10.1.0/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic test
