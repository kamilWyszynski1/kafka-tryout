package main

import (
	"kafka-tryout/cmd/producer"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

func main() {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	wg := &sync.WaitGroup{}
	finish := make(chan struct{})

	cli := producer.NewProducer(log, 10*time.Second, finish, wg, 10)
	cli.Run()
}
