package consumer

import (
	"encoding/json"
	"fmt"
	"kafka-tryout/src/rate"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type consumeFn func(kafka.Message, logrus.FieldLogger) error

func ConsumeCurrenciesFn(m kafka.Message, log logrus.FieldLogger) error {
	curr := rate.SingleCurrency{
		Name: string(m.Key),
	}
	if err := json.Unmarshal(m.Value, &curr.Rate); err != nil {
		return fmt.Errorf("failed to unmarshal rate, %w", err)
	}
	log.Debugf("%+v from goroutine: %s", curr, m.Headers[0].Value)
	// handle message(currency) here
	return nil
}
