package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-tryout/cmd/kafka_server"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const apiURL = "https://api.ratesapi.io/api/latest"

type Producer interface {
	// Run runs producer, it produces messages constantly
	Run()
	// RunCount runs producer, it produces messages <count> times
	RunCount(count int)
}

type handler struct {
	w          *kafka.Writer
	log        logrus.FieldLogger
	sleep      time.Duration
	finish     chan struct{}
	wg         *sync.WaitGroup
	goroutines int
}

func NewProducer(log logrus.FieldLogger, sleep time.Duration, finish chan struct{}, wg *sync.WaitGroup, goroutines int) Producer {
	return &handler{
		w: kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{kafka_server.Address},
			// producer writes one message to one partition at the time, e.g. if we have 3 messages and 4 partitions
			// it would be the output:
			// INFO[0004] writing 1 messages to topic (partition: 0)
			// INFO[0004] writing 1 messages to topic (partition: 2)
			// INFO[0004] writing 1 messages to topic (partition: 1)
			Topic: "currencies",
			//Logger:      log,
			//ErrorLogger: log,
			Balancer: &kafka.LeastBytes{},
		}),
		log:        log,
		sleep:      sleep,
		finish:     finish,
		wg:         wg,
		goroutines: goroutines,
	}
}

func (h *handler) Run() {
	for {
		select {
		case <-h.finish:
			return
		case <-time.After(h.sleep):
			h.log.Info("starting producing currencies")
			// download currencies
			curr, err := getCurrencies()
			if err != nil {
				h.log.WithError(err).Error("failed to get currencies")
				continue
			}
			// divided currencies for specific goroutines
			divided := h.divideCurrencies(curr)
			for goroutineCount, div := range divided {
				// register goroutine
				h.wg.Add(1)
				go func(div []SingleCurrency, goroutineIndex int) {
					h.log.Debugf("goroutine %d starting", goroutineIndex)
					// unregister goroutines
					defer h.wg.Done()
					for _, d := range div {
						// marshal currency rate
						value, err := json.Marshal(d.rate)
						if err != nil {
							h.log.WithError(err).Error("failed to marshal rate")
							continue
						}
						// we could write all messages at once but we don't want to :)
						if err := h.w.WriteMessages(context.Background(),
							kafka.Message{
								Key:   []byte(d.Name),
								Value: value,
								Headers: []kafka.Header{
									{
										Key:   "goroutine",
										Value: []byte(strconv.Itoa(goroutineIndex)),
									},
								},
								Time: time.Now(),
							}); err != nil {

							h.log.WithError(err).Error("failed to write messages")
							continue
						}
					}
				}(div, goroutineCount)
			}
			// wait for all goroutines to end
			h.wg.Wait()
			h.log.Info("write succeeded")
		}
	}
}

func (h *handler) RunCount(count int) {
	messagesByGoroutine := make([]int, h.goroutines)
	for i := 0; i < count; i++ {
		messagesByGoroutine[i%h.goroutines]++
	}

	for goIndex, mCount := range messagesByGoroutine {
		h.wg.Add(1)
		mCount := mCount
		goIndex := goIndex
		go func() {
			for i := 0; i < mCount; i++ {
				msg := fmt.Sprintf("go: %d, count: %d", goIndex, i)
				if err := h.w.WriteMessages(context.Background(),
					//kafka.Message{Value: []byte(msg)},
					kafka.Message{
						Key:   []byte("key"),
						Value: []byte("value" + strconv.Itoa(count)),
						Time:  time.Date(2020, 0, 0, 0, 0, 0, 0, time.UTC),
					},
				); err != nil {
					h.log.WithError(err).Error("failed to write message")
				}
				h.log.Infof("written: %s", msg)
			}
			h.wg.Done()
		}()
	}
}

// divideCurrencies divides currencies into chunks for each goroutine
// e.g.
// 1,2,3,4,5,6,7 for 3 goroutines would be -> [[1,4,7], [2,5], [3,6]]
func (h handler) divideCurrencies(c *Currencies) [][]SingleCurrency {
	val := reflect.Indirect(reflect.ValueOf(c.Rates))
	divided := make([][]SingleCurrency, h.goroutines)

	for i := 0; i < val.NumField(); i++ {
		divided[i%h.goroutines] = append(divided[i%h.goroutines], SingleCurrency{
			Name: val.Type().Field(i).Name,
			rate: rate{
				Base: c.Base,
				Rate: val.Field(i).Float(),
				Date: c.Date,
			},
		})
	}
	return divided
}

func getCurrencies() (*Currencies, error) {
	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get data, %w", err)
	}
	var curr Currencies
	if err := json.NewDecoder(resp.Body).Decode(&curr); err != nil {
		return nil, fmt.Errorf("failed to decode currencies, %w", err)
	}
	return &curr, nil
}
