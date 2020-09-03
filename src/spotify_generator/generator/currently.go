package generator

import (
	"kafka-tryout/src/spotify_generator"
	"time"

	"github.com/zmb3/spotify"
)

func (c *Client) StartGettingCurrentlyPlaying(messageChan chan interface{}) {
	go func() {
		for {
			select {
			case <-time.After(5 * time.Second):
				c.getCurrentlyPlaying(messageChan)
			case <-c.finish:
				c.log.Info("start method finished")
				return
			}
		}
	}()
}

func (c *Client) getCurrentlyPlaying(messageChan chan interface{}) {
	log := c.log.WithField("method", "getCurrentlyPlaying")
	items, err := c.client.PlayerRecentlyPlayedOpt(&spotify.RecentlyPlayedOptions{
		Limit:         50,
		AfterEpochMs:  0,
		BeforeEpochMs: 0,
	})
	if err != nil {
		log.WithError(err).Error("failed to get recently played tracks")
		return
	}

	for _, item := range items {
		// get track artists
		artists, err := c.client.GetArtists(resolveArtists(item.Track.Artists)...)
		if err != nil {
			log.WithError(err).Error("failed to get artists")
			continue
		}

		messageChan <- spotify_generator.CurrentlyPlaying{
			PlayedAt:   item.PlayedAt,
			Artists:    convertArtists(artists),
			TrackName:  item.Track.Name,
			DurationMs: item.Track.Duration,
		}
	}

}

func convertArtists(artists []*spotify.FullArtist) []spotify_generator.Artist {
	ars := make([]spotify_generator.Artist, 0, len(artists))
	for _, artist := range artists {
		ars = append(ars, spotify_generator.Artist{
			Name:       artist.Name,
			Genres:     artist.Genres,
			Followers:  artist.Followers.Count,
			Popularity: artist.Popularity,
		})
	}
	return ars
}

func resolveArtists(artists []spotify.SimpleArtist) []spotify.ID {
	ids := make([]spotify.ID, 0, len(artists))
	for _, artist := range artists {
		ids = append(ids, artist.ID)
	}
	return ids
}
