package main

import (
	"kafka-tryout/src/consumer"
	"kafka-tryout/src/kafka_server"
	"kafka-tryout/src/utils"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func main() {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	logger := log.WithField("application", "Consumer")
	wg := &sync.WaitGroup{}
	finish := make(chan struct{})

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafka_server.Address},
		Topic:   utils.EnvOrDefault("TOPIC", "currencies"),
		// groupID reads from all partitions of given topic
		GroupID: utils.EnvOrDefault("GROUP_ID", "consumer-group"),
		//Logger:      log,
		//ErrorLogger: log,
		MinBytes:    1,    // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.FirstOffset,
	})

	cli := consumer.NewConsumer(logger, r, time.Second, finish, wg, 5, consumer.ConsumeCurrenciesFn)
	cli.Run()

	wg.Wait()
	logger.Info("closing")
}
