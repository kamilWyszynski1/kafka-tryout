package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const apiURL = "https://api.ratesapi.io/api/latest"

// produceFn takes number of goroutines and returns array of messages for each gouroutine
type (
	produceFn func(int) (Messages, error)
	Messages  [][]kafka.Message
)

func (m Messages) Len() int {
	l := 0
	for _, chunk := range m {
		l += len(chunk)
	}
	return l
}

type Producer interface {
	// Run runs producer, it produces messages constantly
	Run()
}

type handler struct {
	w            *kafka.Writer
	log          logrus.FieldLogger
	sleep        time.Duration
	finish       chan struct{}
	wg           *sync.WaitGroup
	goroutines   int
	produceMsgFn produceFn
}

func NewProducer(log logrus.FieldLogger, w *kafka.Writer, sleep time.Duration,
	finish chan struct{}, wg *sync.WaitGroup, goroutines int, fn produceFn) Producer {

	return &handler{
		w:            w,
		log:          log,
		sleep:        sleep,
		finish:       finish,
		wg:           wg,
		goroutines:   goroutines,
		produceMsgFn: fn,
	}
}

func (h *handler) Run() {
	for {
		select {
		case <-h.finish:
			return
		case <-time.After(h.sleep):
			if err := h.handleProducedMessages(h.produceMsgFn); err != nil {
				h.log.WithError(err).Error("failed to handle produced messages")
			}
		}
	}
}

func (h *handler) handleProducedMessages(fn produceFn) error {
	messages, err := fn(h.goroutines)
	if err != nil {
		return fmt.Errorf("failed to produce messages, %w", err)
	}
	ctx := context.Background()
	for i, chunk := range messages {
		h.wg.Add(1)
		// create goroutine to write chunk of messages
		go func(chunk []kafka.Message, log logrus.FieldLogger) {
			log.Info("start handling messages")

			if err := h.w.WriteMessages(ctx, chunk...); err != nil {
				log.WithError(err).Error("failed to write messages")
			}
			h.wg.Done()

		}(chunk, h.log.WithField("goroutine", i))
	}
	h.wg.Wait()
	h.log.Info("messages handled, count: ", messages.Len())
	return nil
}
