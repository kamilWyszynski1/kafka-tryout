package consumer

import (
	"context"
	"kafka-tryout/cmd/kafka_server"
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
			Topic:   kafka_server.Topic,
			// groupID reads from all partitions of given topic
			GroupID: "consumers-group",
			//Logger:      log,
			//ErrorLogger: log,
			MinBytes:    1,    // 10KB
			MaxBytes:    10e6, // 10MB
			StartOffset: -1,
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
		go func() {
			for {
				select {
				case <-h.finish:
					h.r.Close()
					h.wg.Done()
				case <-time.After(h.sleep):
					h.log.Info("reading message")
					m, err := h.r.ReadMessage(context.Background())
					if err != nil {
						h.log.WithError(err).Error("failed to read message")
					} else {
						h.incMsgCounter()
						h.log.Infof("%s %s %s %s", string(m.Key), m.Value, m.Time, m.Headers)
						//h.log.Infof("message at partition: %d, offset %d: %s = %s\n", m.Partition, m.Offset, string(m.Key), string(m.Value))
						h.log.Debugf("m count: %d", h.getMsgCounter())
					}
				}
			}
		}()
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
