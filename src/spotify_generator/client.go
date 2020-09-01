package spotify_generator

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/zmb3/spotify"
)

const (
	playlistCount = 1
	trackCount    = 10
)

// Proposition keeps info of found proposition for user
type Proposition struct {
	TrackName string
	Artists   []string
	Album     string
}

type options struct {
	offset int
	limit  int
}

type Client struct {
	userID   string
	client   *spotify.Client
	log      logrus.FieldLogger
	propChan chan Proposition
	goRCount int
	country  string
	wg       sync.WaitGroup

	currentPlaylist spotify.ID
	// playlistOptions indicates which playlist should be taken
	playlistOptions options
	// trackOptions indicates which playlists from specific playlist should be taken
	trackOptions options
}

func NewClient(userID string, client *spotify.Client, goroutinesCount int) *Client {
	return &Client{
		userID:          userID,
		client:          client,
		log:             logrus.New(),
		goRCount:        goroutinesCount,
		country:         "PL",
		propChan:        make(chan Proposition, goroutinesCount),
		wg:              sync.WaitGroup{},
		currentPlaylist: "",
		// take 1 playlist at the time
		playlistOptions: options{0, playlistCount},
		// take 10 tracks from playlist at the time
		trackOptions: options{0, trackCount},
	}
}

func (c *Client) Start() {
	for i := 0; i < c.goRCount; i++ {
		c.wg.Add(1)
		go func(log logrus.FieldLogger) {
			for {
				select {
				case prop := <-c.propChan:
					log.Infof("%+v", prop)

				}
			}
		}(c.log.WithField("goR", i))
	}
}

// getPropositions gets all os user playlists, searches for tracks, then
// gets all possible combinations of unknown tracks from common albums
//
// we want method to returns only a few proposition at once
// so we've created playlist and tracks options
func (c *Client) GetPropositions() error {
	log := c.log.WithField("method", "getPropositions")
	seenAlbums := make(map[spotify.ID]struct{})

	var playlistID spotify.ID

	if c.currentPlaylist != "" {
		playlistID = c.currentPlaylist
	} else {
		// get user playlists
		pp, err := c.client.GetPlaylistsForUserOpt(c.userID, &spotify.Options{
			Country: &c.country,
			Limit:   &c.playlistOptions.limit,
			Offset:  &c.playlistOptions.offset,
		})
		if err != nil {
			return fmt.Errorf("failed to get playlist, %w", err)
		}
		// we are taking only one playlist at the time so we can do it like that
		playlistID = pp.Playlists[0].ID
	}

	ptp, err := c.client.GetPlaylistTracksOpt(playlistID, &spotify.Options{
		Country: &c.country,
		Limit:   &c.trackOptions.limit,
		Offset:  &c.trackOptions.offset,
	}, "")

	if err != nil {
		return fmt.Errorf("failed to get playlist tracks, %w", err)
	}
	for _, track := range ptp.Tracks {
		album := track.Track.Album
		if _, ok := seenAlbums[album.ID]; ok {
			log.Infof("%s album already seen", album.Name)
			continue
		}
		at, err := c.client.GetAlbumTracks(album.ID)
		if err != nil {
			log.WithError(err).Error("failed to get albums tracks")
			continue
		}

		// write seen album
		seenAlbums[album.ID] = struct{}{}
		for _, t := range at.Tracks {
			c.propChan <- Proposition{
				t.Name,
				trimArtists(t.Artists),
				album.Name,
			}
		}
	}
	return nil
}

func trimArtists(artists []spotify.SimpleArtist) []string {
	a := make([]string, 0, len(artists))
	for _, artist := range artists {
		a = append(a, artist.Name)
	}
	return a
}
