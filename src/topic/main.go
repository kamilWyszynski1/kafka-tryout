package main

import (
	"context"
	"fmt"
	"kafka-tryout/src/kafka_server"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func main() {
	if err := CreateTopic(logrus.New()); err != nil {
		panic(err)
	}
}

func CreateTopic(log logrus.FieldLogger) error {
	cli := kafka.Client{
		Addr:      kafka.TCP(kafka_server.Address),
		Timeout:   10 * time.Second,
		Transport: nil,
	}
	resp, err := cli.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Addr: kafka.TCP(kafka_server.Address),
		Topics: []kafka.TopicConfig{
			{
				Topic:         "topic",
				NumPartitions: 10,
				//  replication factor: x larger than available brokers: 1
				ReplicationFactor:  1,
				ReplicaAssignments: nil,
				// https://docs.confluent.io/current/installation/configuration/topic-configs.html
				ConfigEntries: nil,
			},
		},
		ValidateOnly: false,
	})
	if err != nil {
		return fmt.Errorf("failed to craete topic, %w", err)
	}
	log.Infof("create topic resp: %+v", resp)
	return nil
}
