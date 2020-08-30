package consumer

import (
	"context"
	"encoding/json"
	"kafka-tryout/cmd/kafka_server"
	"kafka-tryout/cmd/producer"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Consumer interface {
	Run()
}

type handler struct {
	r               *kafka.Reader
	log             logrus.FieldLogger
	sleep           time.Duration
	finish          chan struct{}
	wg              *sync.WaitGroup
	goroutines      int
	messagesCounter int
	mtx             sync.Mutex
}

func NewConsumer(log logrus.FieldLogger, sleep time.Duration, finish chan struct{}, wg *sync.WaitGroup, goroutinesCount int) Consumer {
	return &handler{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{kafka_server.Address},
			Topic:   "currencies",
			// groupID reads from all partitions of given topic
			GroupID: "consumers-group",
			//Logger:      log,
			//ErrorLogger: log,
			MinBytes:    1,    // 10KB
			MaxBytes:    10e6, // 10MB
			StartOffset: kafka.FirstOffset,
		}),
		log:        log,
		sleep:      sleep,
		finish:     finish,
		wg:         wg,
		goroutines: goroutinesCount,
	}
}

func (h *handler) Run() {
	for i := 0; i < h.goroutines; i++ {
		h.wg.Add(1)
		go func(goroutineIndex int) {
			for {
				select {
				case <-h.finish:
					h.r.Close()
					h.wg.Done()
				case <-time.After(h.sleep):
					h.log.Infof("goroutine: %d reading messages", goroutineIndex)
					m, err := h.r.ReadMessage(context.Background())
					if err != nil {
						h.log.WithError(err).Error("failed to read message")
					} else {
						curr := producer.SingleCurrency{
							Name: string(m.Key),
						}
						if err := json.Unmarshal(m.Value, &curr.Rate); err != nil {
							h.log.WithError(err).Error("failed to unmarshal rate")
							continue
						}
						h.log.Debugf("%+v from goroutine: %s", curr, m.Headers[0].Value)
						// handle message(currency) here
					}
				}
			}
		}(i)
	}
}

func (h *handler) incMsgCounter() {
	h.mtx.Lock()
	h.messagesCounter++
	h.mtx.Unlock()
}

func (h *handler) getMsgCounter() int {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	return h.messagesCounter
}
