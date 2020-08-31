package producer

import (
	"encoding/json"
	"fmt"
	"kafka-tryout/src/rate"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

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

func ProduceCurrenciesFn(goroutineCount int) (Messages, error) {
	curr, err := getCurrencies()
	if err != nil {
		return nil, fmt.Errorf("failed to get currencies, %w", err)
	}

	// divided currencies for specific goroutines
	divided := divideCurrencies(curr, goroutineCount)

	messages := make([][]kafka.Message, 0, len(divided))

	for i, div := range divided {
		m := make([]kafka.Message, 0, len(div))
		for _, d := range div {
			// marshal currency rate
			value, err := json.Marshal(d.Rate)
			if err != nil {
				continue
			}
			m = append(m, kafka.Message{
				Key:   []byte(d.Name),
				Value: value,
				Headers: []kafka.Header{
					{
						Key:   "goroutine",
						Value: []byte(strconv.Itoa(i)),
					},
				},
				Time: time.Now(),
			})
		}
		messages = append(messages, m)
	}
	return messages, nil
}

func getCurrencies() (*rate.Currencies, error) {
	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get data, %w", err)
	}
	var curr rate.Currencies
	if err := json.NewDecoder(resp.Body).Decode(&curr); err != nil {
		return nil, fmt.Errorf("failed to decode currencies, %w", err)
	}
	return &curr, nil
}

// divideCurrencies divides currencies into chunks for each goroutine
// e.g.
// 1,2,3,4,5,6,7 for 3 goroutines would be -> [[1,4,7], [2,5], [3,6]]
func divideCurrencies(c *rate.Currencies, goroutinesCount int) [][]rate.SingleCurrency {
	val := reflect.Indirect(reflect.ValueOf(c.Rates))
	divided := make([][]rate.SingleCurrency, goroutinesCount)

	for i := 0; i < val.NumField(); i++ {
		divided[i%goroutinesCount] = append(divided[i%goroutinesCount], rate.SingleCurrency{
			Name: val.Type().Field(i).Name,
			Rate: rate.Rate{
				Base: c.Base,
				Rate: val.Field(i).Float(),
				Date: c.Date,
			},
		})
	}
	return divided
}
