package handler

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

func RunProducer(log logrus.FieldLogger, sleep time.Duration, finish chan struct{}, wg *sync.WaitGroup) {
	// make a writer that produces to topic-A, using the least-bytes distribution
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       Topic,
		Logger:      log,
		ErrorLogger: log,
		Balancer:    &kafka.LeastBytes{},
	})

	wg.Add(1)
	go func() {
		for {
			select {
			case <-finish:
				w.Close()
				wg.Done()
			case <-time.After(sleep):
				log.Info("writiing message")
				if err := w.WriteMessages(context.Background(),
					kafka.Message{Value: []byte("one!")},
					kafka.Message{Value: []byte("two!")},
					kafka.Message{Value: []byte("three!")},
				); err != nil {
					log.WithError(err).Error("failed to write message")
				}
			}
		}
	}()
}
