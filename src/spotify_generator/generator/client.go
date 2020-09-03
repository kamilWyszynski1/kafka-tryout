package generator

import (
	"github.com/sirupsen/logrus"
	"github.com/zmb3/spotify"
)

const (
	_playlistCount = 1
	_trackCount    = 25
)

type propositionOptions struct {
	currentPlaylist     spotify.ID
	tracksOnPlaylistLen int
	userPlaylistLen     int

	// playlistOptions indicates which playlist should be taken
	playlistOptions options
	// trackOptions indicates which playlists from specific playlist should be taken
	trackOptions options
}

type currentlyPlayingOptions struct {
	limit        int
	afterEpochMs int
}

type options struct {
	offset int
	limit  int
}

// Client handles querying user's playlists, tracks & albums
type Client struct {
	userID  string
	country string
	client  *spotify.Client

	log      logrus.FieldLogger
	goRCount int

	propOpts *propositionOptions
	currOpts *currentlyPlayingOptions

	finish chan struct{}
}

func NewClient(log logrus.FieldLogger, client *spotify.Client, userID string, goroutinesCount int, finish chan struct{}) *Client {
	return &Client{
		userID: userID,
		client: client,

		log:      log,
		goRCount: goroutinesCount,
		country:  "PL",

		propOpts: &propositionOptions{
			currentPlaylist:     "",
			tracksOnPlaylistLen: 0,
			userPlaylistLen:     0,
			playlistOptions:     options{0, _playlistCount},
			trackOptions:        options{0, _trackCount},
		},

		currOpts: &currentlyPlayingOptions{
			limit: 10,
		},

		finish: finish,
	}
}
