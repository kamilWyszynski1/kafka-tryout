#!/bin/bash

# This command will start an unnamed instance of spotify/kafka linked to the kafka service, start a consumer,
# display existing messages from the test topic, and wait for new messages until you quit (which destroys the container):
docker run -it --rm --link kafka spotify/kafka /opt/kafka_2.11-0.10.1.0/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test --from-beginning
