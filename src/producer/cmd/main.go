package main

import (
	"kafka-tryout/src/kafka_server"
	"kafka-tryout/src/producer"
	"kafka-tryout/src/utils"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func main() {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	logger := log.WithField("application", "Producer")
	wg := &sync.WaitGroup{}
	finish := make(chan struct{})

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafka_server.Address},
		// producer writes one message to one partition at the time, e.g. if we have 3 messages and 4 partitions
		// it would be the output:
		// INFO[0004] writing 1 messages to topic (partition: 0)
		// INFO[0004] writing 1 messages to topic (partition: 2)
		// INFO[0004] writing 1 messages to topic (partition: 1)
		Topic: utils.EnvOrDefault("TOPIC", "currencies"),
		//Logger:      log,
		//ErrorLogger: log,
		Balancer: &kafka.LeastBytes{},
	})

	cli := producer.NewProducer(logger, w, 10*time.Second, finish, wg, 10, producer.ProduceCurrenciesFn)
	cli.Run()

	wg.Wait()
	logger.Info("closing")
}
