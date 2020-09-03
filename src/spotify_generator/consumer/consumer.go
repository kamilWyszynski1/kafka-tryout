package consumer

import (
	"context"
	"encoding/json"
	"kafka-tryout/src/spotify_generator"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const _chunkSize = 20

type PropositionHandler interface {
	// Consume gets Proposition and handles it
	Consume(chan spotify_generator.Proposition) error
}

type kafkaClient struct {
	spotifyWriter *kafka.Writer
	currWriter    *kafka.Writer

	ctx context.Context
	log logrus.FieldLogger

	index int

	finish chan struct{}

	spotifyChunk []kafka.Message
	currChunk    []kafka.Message

	chunkSize int
	wg        *sync.WaitGroup
}

func NewKafkaClient(spotifyW, currentlyW *kafka.Writer, log logrus.FieldLogger, ctx context.Context, index, chunkSize int, finish chan struct{}, wg *sync.WaitGroup) *kafkaClient {
	return &kafkaClient{
		spotifyWriter: spotifyW,
		currWriter:    currentlyW,
		log:           log,
		ctx:           ctx,
		index:         index,
		finish:        finish,
		chunkSize:     chunkSize,
		spotifyChunk:  make([]kafka.Message, chunkSize),
		currChunk:     make([]kafka.Message, chunkSize),
		wg:            wg,
	}
}

// Consume registers given channel and process data from it
func (k *kafkaClient) Consume(messageChan chan interface{}) {
	k.wg.Add(1)
	spotifyCounter := 0
	currCounter := 0
	k.log.Info("start consuming data")

	go func() {
		for {
			select {
			case <-k.finish:
				k.log.Info("graceful shutdown")
				k.wg.Done()
				return
			case data := <-messageChan:
				var m kafka.Message

				// handle data, what ever type it is
				switch data.(type) {
				// handle Proposition
				case spotify_generator.Proposition:
					k.log.Debug("handling proposition")
					m = k.handleProposition(data.(spotify_generator.Proposition))
					// process kafka message
					if spotifyCounter >= k.chunkSize {
						go k.sendSpotify(k.spotifyChunk)
						spotifyCounter = 0
					} else {
						k.spotifyChunk[spotifyCounter] = m
						spotifyCounter++
					}
				case spotify_generator.CurrentlyPlaying:
					k.log.Debug("handling currently playing")
					m = k.handleCurrentlyPlaying(data.(spotify_generator.CurrentlyPlaying))
					// process kafka message
					if currCounter >= k.chunkSize {
						go k.sendCurr(k.currChunk)
						currCounter = 0
					} else {
						k.currChunk[currCounter] = m
						currCounter++
					}
				}

			}
		}
	}()
}

// handleProposition generate kafka.Message from Proposition
func (k kafkaClient) handleProposition(p spotify_generator.Proposition) kafka.Message {
	m := kafka.Message{
		Topic: "spotify",
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
	return m
}

func (k *kafkaClient) handleCurrentlyPlaying(c spotify_generator.CurrentlyPlaying) kafka.Message {
	m := kafka.Message{
		Key:   []byte("track-name"),
		Value: []byte(c.TrackName),
		Headers: []kafka.Header{
			{
				Key:   "played-at",
				Value: []byte(strconv.Itoa(c.PlayedAt.Second())),
			},
			{
				Key:   "duration",
				Value: []byte(strconv.Itoa(c.DurationMs)),
			},
		},
		Time: time.Now(),
	}
	for _, artist := range c.Artists {
		b, err := json.Marshal(artist)
		if err != nil {
			k.log.WithError(err).Error("failed to marshal artist")
			return m
		}
		m.Headers = append(m.Headers, kafka.Header{
			Key:   "artist",
			Value: b,
		})
	}
	return m
}

// sendSpotify sends given messages to kafka cluster
func (k kafkaClient) sendSpotify(messages []kafka.Message) {
	if err := k.spotifyWriter.WriteMessages(k.ctx, messages...); err != nil {
		k.log.WithFields(logrus.Fields{
			"method": "sendSpotify",
			"len":    len(messages),
		}).WithError(err).Error("failed to write messages to kafka")
	} else {
		k.log.Infof("successfully writen spotify messages: %d", len(messages))
	}
}

// sendCurr sends given messages to kafka cluster
func (k kafkaClient) sendCurr(messages []kafka.Message) {
	if err := k.currWriter.WriteMessages(k.ctx, messages...); err != nil {
		k.log.WithFields(logrus.Fields{
			"method": "sendCurr",
			"len":    len(messages),
		}).WithError(err).Error("failed to write messages to kafka")
	} else {
		k.log.Infof("successfully writen curr messages: %d", len(messages))
	}
}
