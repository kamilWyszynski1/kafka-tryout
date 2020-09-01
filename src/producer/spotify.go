package producer

import (
	"context"
	"fmt"
	"log"

	"github.com/zmb3/spotify"
	"golang.org/x/oauth2/clientcredentials"
)

func ProduceSpotifyFn(goroutinesCount int) (Messages, error) {
	config := &clientcredentials.Config{
		ClientID:     "0d490b48a7014abbbd2dcad0ecfdbe1e",
		ClientSecret: "a40da0f1b6b34de68af833aa7a24522a",
		TokenURL:     spotify.TokenURL,
	}
	token, err := config.Token(context.Background())
	if err != nil {
		log.Fatalf("couldn't get token: %v", err)
	}

	client := spotify.Authenticator{}.NewClient(token)

	fmt.Println(client.CurrentUser())

	return nil, nil
}
