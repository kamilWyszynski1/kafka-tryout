package handler

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"time"
)

func CreateTopic(log logrus.FieldLogger) error {
	cli := kafka.Client{
		Addr:      kafka.TCP("localhost:9092"),
		Timeout:   10 * time.Second,
		Transport: nil,
	}
	resp, err := cli.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Addr: kafka.TCP("localhost:9092"),
		Topics: []kafka.TopicConfig{
			{
				Topic:              "topic",
				NumPartitions:      10,
				ReplicationFactor:  1,
				ReplicaAssignments: nil,
				ConfigEntries:      nil,
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
