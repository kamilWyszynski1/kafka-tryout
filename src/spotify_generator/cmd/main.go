// This example demonstrates how to authenticate with Spotify using the authorization code flow.
// In order to run this example yourself, you'll need to:
//
//  1. Register an application at: https://developer.spotify.com/my-applications/
//       - Use "http://localhost:8080/callback" as the redirect URI
//  2. Set the SPOTIFY_ID environment variable to the client ID you got in step 1.
//  3. Set the SPOTIFY_SECRET environment variable to the client secret from step 1.
package main

import (
	"context"
	"fmt"
	"kafka-tryout/src/kafka_server"
	"kafka-tryout/src/spotify_generator/generator"
	"kafka-tryout/src/spotify_generator/producer"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/segmentio/kafka-go"

	"github.com/sirupsen/logrus"
	"github.com/zmb3/spotify"
)

// redirectURI is the OAuth redirect URI for the application.
// You must register an application at Spotify's developer portal
// and enter this value.
const redirectURI = "http://localhost:8080/callback"

var (
	auth  = spotify.NewAuthenticator(redirectURI, spotify.ScopeUserReadPrivate, spotify.ScopeUserReadRecentlyPlayed)
	ch    = make(chan *spotify.Client)
	state = "abc123"
)

func main() {
	// first start an HTTP server
	http.HandleFunc("/callback", completeAuth)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Got request for:", r.URL.String())
	})
	go http.ListenAndServe(":8080", nil)

	url := auth.AuthURL(state)
	fmt.Println("Please logger in to Spotify by visiting the following page in your browser:", url)

	// wait for auth to complete
	client := <-ch

	// use the client to make calls that require authorization
	user, err := client.CurrentUser()
	if err != nil {
		log.Fatal(err)
	}

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	spotifyW := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafka_server.Address},
		// producer writes one message to one partition at the time, e.g. if we have 3 messages and 4 partitions
		// it would be the output:
		// INFO[0004] writing 1 messages to topic (partition: 0)
		// INFO[0004] writing 1 messages to topic (partition: 2)
		// INFO[0004] writing 1 messages to topic (partition: 1)
		Topic: "spotify",
		//Logger:      log,
		//ErrorLogger: log,
		Balancer: &kafka.LeastBytes{},
	})

	currW := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafka_server.Address},
		// producer writes one message to one partition at the time, e.g. if we have 3 messages and 4 partitions
		// it would be the output:
		// INFO[0004] writing 1 messages to topic (partition: 0)
		// INFO[0004] writing 1 messages to topic (partition: 2)
		// INFO[0004] writing 1 messages to topic (partition: 1)
		Topic: "currently-playing",
		//Logger:      log,
		//ErrorLogger: log,
		Balancer: &kafka.LeastBytes{},
	})

	finish := make(chan struct{})
	signalChannel := make(chan os.Signal, 2)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChannel
		close(finish)
	}()

	var (
		goroutinesCount = 1
		messageChan     = make(chan interface{}, goroutinesCount)
	)

	cli := generator.NewClient(logger, client, user.ID, goroutinesCount, finish)
	//cli.StartGettingPropositions(messageChan)
	cli.StartGettingCurrentlyPlaying(messageChan)

	ctx := context.Background()
	wg := sync.WaitGroup{}
	for i := 0; i < 2*goroutinesCount; i++ {
		pr := producer.NewKafkaClient(spotifyW, currW, logger.WithField("goR", i), ctx, i, 5, finish, &wg)
		pr.Consume(messageChan)
	}
	wg.Wait()
	logger.Info("spotify generator finished")
}

func completeAuth(w http.ResponseWriter, r *http.Request) {
	tok, err := auth.Token(state, r)
	if err != nil {
		http.Error(w, "Couldn't get token", http.StatusForbidden)
		log.Fatal(err)
	}
	if st := r.FormValue("state"); st != state {
		http.NotFound(w, r)
		log.Fatalf("State mismatch: %s != %s\n", st, state)
	}
	// use the token to get an authenticated client
	client := auth.NewClient(tok)
	fmt.Fprintf(w, "Login Completed!")
	ch <- &client
}
