package main

import (
	"kafka-tryout/cmd/consumer"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

func main() {
	log := logrus.New()
	wg := &sync.WaitGroup{}
	finish := make(chan struct{})
	log.SetLevel(logrus.DebugLevel)

	cli := consumer.NewConsumer(log, time.Second, finish, wg, 5)
	cli.Run()

	wg.Wait()
	log.Info("closing consumer")
}
