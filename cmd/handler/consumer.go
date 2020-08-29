package handler

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

func RunConsumer(log logrus.FieldLogger, sleep time.Duration, finish chan struct{}, wg *sync.WaitGroup) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       Topic,
		Partition:   Partition,
		Logger:      log,
		ErrorLogger: log,
		MinBytes:    1,    // 10KB
		MaxBytes:    10e6, // 10MB
	})

	wg.Add(1)
	go func() {
		for {
			select {
			case <-finish:
				r.Close()
				wg.Done()
			case <-time.After(sleep):
				log.Info("reading message")
				m, err := r.ReadMessage(context.Background())
				if err != nil {
					log.WithError(err).Error("failed to read message")
				} else {
					fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
				}
			}
		}
	}()
}
