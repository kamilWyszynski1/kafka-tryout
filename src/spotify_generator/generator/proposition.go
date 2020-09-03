package generator

import (
	"kafka-tryout/src/spotify_generator"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zmb3/spotify"
)

// StartGettingPropositions start getting proposition periodically
func (c *Client) StartGettingPropositions(messageChan chan interface{}) {
	go func() {
		for {
			select {
			case <-time.After(5 * time.Second):
				c.getPropositions(messageChan)
			case <-c.finish:
				c.log.Info("start method finished")
				return
			}
		}
	}()
}

// getPropositions gets all os user playlists, searches for tracks, then
// gets all possible combinations of unknown tracks from common albums
//
// we want method to returns only a few proposition at once
// so we've created playlist and tracks options
func (c *Client) getPropositions(propChan chan interface{}) {
	log := c.log.WithFields(logrus.Fields{
		"method": "getPropositions",
		"userID": c.userID,
	})

	var seenAlbums = make(map[spotify.ID]struct{})

	opts := c.propOpts

	if opts.currentPlaylist == "" {
		// get user playlists
		pp, err := c.client.GetPlaylistsForUserOpt(c.userID, &spotify.Options{
			Country: &c.country,
			Limit:   &opts.playlistOptions.limit,
			Offset:  &opts.playlistOptions.offset,
		})
		if err != nil {
			log.WithError(err).Error("failed to get playlists")
			return
		}
		// we are taking only one playlist at the time so we can do it like that
		// assign playlist values
		opts.currentPlaylist = pp.Playlists[0].ID
		opts.userPlaylistLen = pp.Total
	}

	log = log.WithField("playlistID", opts.currentPlaylist)

	ptp, err := c.client.GetPlaylistTracksOpt(opts.currentPlaylist, &spotify.Options{
		Country: &c.country,
		Limit:   &opts.trackOptions.limit,
		Offset:  &opts.trackOptions.offset,
	}, "")
	if err != nil {
		log.WithError(err).Error("failed to get playlist's tracks")
		return
	}

	// set total elements of playlist
	if opts.tracksOnPlaylistLen == 0 {
		opts.tracksOnPlaylistLen = ptp.Total
	}

	log.Infof("currently checking: (playlist: %d, total: %d), track: %d",
		opts.playlistOptions.offset, opts.tracksOnPlaylistLen, opts.trackOptions.offset)

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
			propChan <- spotify_generator.Proposition{
				Meta: spotify_generator.Meta{
					PlaylistInx: opts.playlistOptions.offset,
					TrackInx:    opts.trackOptions.offset + i,
					PlaylistID:  opts.currentPlaylist.String(),
					TrackName:   track.Track.Name,
				},
				TrackName: t.Name,
				Artists:   trimArtists(t.Artists),
				Album:     album.Name,
			}
		}
	}

	// if we queried all of tracks from playlist go to the next one
	if opts.trackOptions.offset+len(ptp.Tracks) >= opts.tracksOnPlaylistLen {
		// go from the very beginning
		if opts.playlistOptions.offset+1 == opts.userPlaylistLen {
			c.resetPlaylistValues()
			opts.playlistOptions.offset = 0
			opts.trackOptions.offset = 0
			opts.userPlaylistLen = 0

		}
		// reset all saved values and increase playlist offset
		opts.playlistOptions.offset += 1
		c.resetPlaylistValues()
	} else {
		opts.trackOptions.offset += _trackCount
	}
}

func (c *Client) resetPlaylistValues() {
	c.propOpts.trackOptions.offset = 0
	c.propOpts.tracksOnPlaylistLen = 0
	c.propOpts.currentPlaylist = ""
}

func trimArtists(artists []spotify.SimpleArtist) []string {
	a := make([]string, 0, len(artists))
	for _, artist := range artists {
		a = append(a, artist.Name)
	}
	return a
}
