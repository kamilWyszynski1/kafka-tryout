package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Consumer interface {
	Run()
}

type handler struct {
	r      *kafka.Reader
	log    logrus.FieldLogger
	sleep  time.Duration
	finish chan struct{}
	wg     *sync.WaitGroup

	goroutines int

	consumeMsgFn consumeFn
}

func NewConsumer(log logrus.FieldLogger, r *kafka.Reader, sleep time.Duration,
	finish chan struct{}, wg *sync.WaitGroup, goroutinesCount int, fn consumeFn) Consumer {
	return &handler{
		r:            r,
		log:          log,
		sleep:        sleep,
		finish:       finish,
		wg:           wg,
		goroutines:   goroutinesCount,
		consumeMsgFn: fn,
	}
}

func (h *handler) Run() {
	for i := 0; i < h.goroutines; i++ {
		h.wg.Add(1)
		log := h.log.WithField("goroutine", i)
		go func() {
			for {
				select {
				case <-h.finish:
					h.r.Close()
					h.wg.Done()
				case <-time.After(h.sleep):
					log.Infof("reading message")
					m, err := h.r.ReadMessage(context.Background())
					if err != nil {
						log.WithError(err).Error("failed to read message")
					} else {
						// consume message
						if err := h.consumeMsgFn(m, log); err != nil {
							log.WithError(err).Error("failed to consume message")
						}
					}
				}
			}
		}()
	}
}
