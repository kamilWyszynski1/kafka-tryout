package spotify_generator

import (
	"context"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const _chunkSize = 20

type PropositionHandler interface {
	// Consume gets Proposition and handles it
	Consume(chan Proposition) error
}

type kafkaClient struct {
	cli *kafka.Writer
	ctx context.Context
	log logrus.FieldLogger

	index int

	finish chan struct{}

	chunk     []kafka.Message
	chunkSize int
}

func NewKafkaClient(cli *kafka.Writer, log logrus.FieldLogger, ctx context.Context, index, chunkSize int, finish chan struct{}) *kafkaClient {
	return &kafkaClient{
		cli:       cli,
		log:       log,
		ctx:       ctx,
		index:     index,
		finish:    finish,
		chunkSize: chunkSize,
		chunk:     make([]kafka.Message, chunkSize),
	}
}

func (k *kafkaClient) Consume(propChan chan Proposition) {
	counter := 0
	k.log.Info("start consuming data")
	for {
		select {
		case <-k.finish:
			return
		case p := <-propChan:
			m := kafka.Message{
				Key:   []byte(p.TrackName),
				Value: []byte(p.Album),
				Headers: []kafka.Header{
					{
						Key:   "goroutine",
						Value: []byte(strconv.Itoa(k.index)),
					},
				},
				Time: time.Now(),
			}

			for _, artist := range p.Artists {
				m.Headers = append(m.Headers, kafka.Header{
					Key:   "artist",
					Value: []byte(artist),
				})
			}
			for k, v := range p.GetMeta() {
				m.Headers = append(m.Headers, kafka.Header{
					Key:   k,
					Value: []byte(v),
				})
			}
			if counter >= k.chunkSize {
				go k.send(k.chunk)
				counter = 0
			} else {
				k.chunk[counter] = m
				counter++
			}
		}
	}
}

func (k kafkaClient) send(messages []kafka.Message) {
	if err := k.cli.WriteMessages(k.ctx, messages...); err != nil {
		k.log.WithFields(logrus.Fields{
			"method": "send",
			"len":    len(messages),
		}).WithError(err).Error("failed to write messages to kafka")
	} else {
		k.log.Infof("succesfully writen messages: %d", len(messages))
	}
}
