package spotify_generator

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/sirupsen/logrus"
	"github.com/zmb3/spotify"
)

const (
	_playlistCount = 1
	_trackCount    = 25
)

type meta struct {
	playlistInx, trackInx int
	playlistID, trackName string
}

// Proposition keeps info of found proposition for user
type Proposition struct {
	meta
	TrackName string
	Artists   []string
	Album     string
}

func (p Proposition) GetMeta() map[string]string {
	m := make(map[string]string, 4)
	m["playlistInx"] = strconv.Itoa(p.meta.playlistInx)
	m["trackInx"] = strconv.Itoa(p.meta.trackInx)
	m["playlistID"] = p.meta.playlistID
	m["trackName"] = p.meta.trackName
	return m
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

	kafkaCli *kafka.Writer

	log      logrus.FieldLogger
	propChan chan Proposition
	goRCount int
	wg       sync.WaitGroup

	currentPlaylist     spotify.ID
	tracksOnPlaylistLen int
	userPlaylistLen     int

	// playlistOptions indicates which playlist should be taken
	playlistOptions options
	// trackOptions indicates which playlists from specific playlist should be taken
	trackOptions options
	finish       chan struct{}
}

func NewClient(log logrus.FieldLogger, w *kafka.Writer, userID string, client *spotify.Client, goroutinesCount int, finish chan struct{}) *Client {
	return &Client{
		userID: userID,
		client: client,

		kafkaCli: w,

		log:      log,
		goRCount: goroutinesCount,
		country:  "PL",
		propChan: make(chan Proposition, goroutinesCount),
		wg:       sync.WaitGroup{},

		// currentPlaylist and tracksOnPlaylistLen are used to query only a part of tracks
		currentPlaylist:     "",
		tracksOnPlaylistLen: 0,

		finish: finish,

		// take 1 playlist at the time
		playlistOptions: options{0, _playlistCount},
		// take 10 tracks from playlist at the time
		trackOptions: options{0, _trackCount},
	}
}

func (c *Client) Start() {
	for i := 0; i < c.goRCount; i++ {
		ctx := context.Background()
		c.wg.Add(1)
		go func(inx int, wg *sync.WaitGroup) {
			log := c.log.WithField("goR", inx)

			consumer := NewKafkaClient(c.kafkaCli, c.log.WithField("goR", inx), ctx, inx, 5, c.finish)
			consumer.Consume(c.propChan)

			log.Info("finish work")
			wg.Done()
		}(i, &c.wg)
	}
	go func() {
		for {
			c.getPropositions()
			time.Sleep(time.Minute)
		}
	}()
	c.wg.Wait()
	c.log.Info("start method finished")
}

// getPropositions gets all os user playlists, searches for tracks, then
// gets all possible combinations of unknown tracks from common albums
//
// we want method to returns only a few proposition at once
// so we've created playlist and tracks options
func (c *Client) getPropositions() {
	log := c.log.WithFields(logrus.Fields{
		"method": "getPropositions",
		"userID": c.userID,
	})

	var seenAlbums = make(map[spotify.ID]struct{})

	if c.currentPlaylist == "" {
		// get user playlists
		pp, err := c.client.GetPlaylistsForUserOpt(c.userID, &spotify.Options{
			Country: &c.country,
			Limit:   &c.playlistOptions.limit,
			Offset:  &c.playlistOptions.offset,
		})
		if err != nil {
			log.WithError(err).Error("failed to get playlists")
			return
		}
		// we are taking only one playlist at the time so we can do it like that
		// assign playlist values
		c.currentPlaylist = pp.Playlists[0].ID
		c.userPlaylistLen = pp.Total
	}

	log = log.WithField("playlistID", c.currentPlaylist)

	ptp, err := c.client.GetPlaylistTracksOpt(c.currentPlaylist, &spotify.Options{
		Country: &c.country,
		Limit:   &c.trackOptions.limit,
		Offset:  &c.trackOptions.offset,
	}, "")
	if err != nil {
		log.WithError(err).Error("failed to get playlist's tracks")
		return
	}

	// set total elements of playlist
	if c.tracksOnPlaylistLen == 0 {
		c.tracksOnPlaylistLen = ptp.Total
	}

	log.Infof("currently checking: (playlist: %d, total: %d), track: %d", c.playlistOptions.offset, c.tracksOnPlaylistLen, c.trackOptions.offset)

	for i, track := range ptp.Tracks {
		album := track.Track.Album
		log = log.WithFields(logrus.Fields{"trackID": track.Track.ID, "album": album.Name})
		if _, ok := seenAlbums[album.ID]; ok {
			log.Infof("album: %s already seen", album.Name)
			continue
		}
		at, err := c.client.GetAlbumTracks(album.ID)
		if err != nil {
			log.WithError(err).Error("failed to get albums tracks")
			continue
		}

		log.Infof("checkin track nr: %d, has %d similar tracks", i, len(at.Tracks))
		// write seen album
		seenAlbums[album.ID] = struct{}{}
		for _, t := range at.Tracks {
			c.propChan <- Proposition{
				meta{
					playlistInx: c.playlistOptions.offset,
					trackInx:    c.trackOptions.offset + i,
					playlistID:  c.currentPlaylist.String(),
					trackName:   track.Track.Name,
				},
				t.Name,
				trimArtists(t.Artists),
				album.Name,
			}
		}
	}

	// if we queried all of tracks from playlist go to the next one
	if c.trackOptions.offset+len(ptp.Tracks) >= c.tracksOnPlaylistLen {
		// go from the very beginning
		if c.playlistOptions.offset+1 == c.userPlaylistLen {
			c.resetPlaylistValues()
			c.playlistOptions.offset = 0
			c.trackOptions.offset = 0
			c.userPlaylistLen = 0

		}
		// reset all saved values and increase playlist offset
		c.playlistOptions.offset += 1
		c.resetPlaylistValues()
	} else {
		c.trackOptions.offset += _trackCount
	}
}

func (c *Client) resetPlaylistValues() {
	c.trackOptions.offset = 0
	c.tracksOnPlaylistLen = 0
	c.currentPlaylist = ""
}

func trimArtists(artists []spotify.SimpleArtist) []string {
	a := make([]string, 0, len(artists))
	for _, artist := range artists {
		a = append(a, artist.Name)
	}
	return a
}
