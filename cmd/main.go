package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"kafka-tryout/cmd/consumer"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var (
	topic     = "test"
	partition = 1
	log       = logrus.New()
)

func main() {
	log := logrus.New()
	wg := sync.WaitGroup{}
	finish := make(chan struct{})

	SetupCloseHandler(finish)

	//if err := kafka_server.CreateTopic(log); err != nil {
	//	log.WithError(err).Fatal("")
	//}
	consumer.RunConsumer(log, 3*time.Second, finish, &wg)
	//producer.RunProducer(log, 3*time.Second, finish, &wg)
	wg.Wait()
	log.Info("closing program")
}

// SetupCloseHandler creates a 'listener' on a new goroutine which will notify the
// program if it receives an interrupt from the OS. We then handle this by calling
// our clean up procedure and exiting the program.
func SetupCloseHandler(finish chan struct{}) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		close(finish)
	}()
}

func writeMessages() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.WithError(err).Fatal("failed to connect to kafka")
	}

	if err = conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		log.WithError(err).Fatal("failed to set write deadline")
	}
	length, err := conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		log.WithError(err).Fatal("failed to write messages")
	}

	log.Info("we have written messages succesfully! %d", length)

	if err := conn.Close(); err != nil {
		log.WithError(err).Fatal("failed to close connection")
	}
}

func readMessages() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.WithError(err).Fatal("failed to connect to kafka")
	}

	if err = conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		log.WithError(err).Fatal("failed to set write deadline")
	}
	batch := conn.ReadBatch(0, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
	}

	if err := conn.Close(); err != nil {
		log.WithError(err).Fatal("failed to close connection")
	}
	if err := batch.Close(); err != nil {
		log.WithError(err).Fatal("failed to close connection")
	}
}
