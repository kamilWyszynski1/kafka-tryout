package spotify_generator

import (
	"strconv"
	"time"
)

type Generator interface {
	StartGettingPropositions(chan interface{})
	StartGettingCurrentlyPlaying(messageChan chan interface{})
}

type Meta struct {
	PlaylistInx, TrackInx int
	PlaylistID, TrackName string
}

// Proposition keeps info of found proposition for user
type Proposition struct {
	Meta
	TrackName string
	Artists   []string
	Album     string
}

func (p Proposition) GetMeta() map[string]string {
	m := make(map[string]string, 4)
	m["PlaylistInx"] = strconv.Itoa(p.Meta.PlaylistInx)
	m["TrackInx"] = strconv.Itoa(p.Meta.TrackInx)
	m["PlaylistID"] = p.Meta.PlaylistID
	m["TrackName"] = p.Meta.TrackName
	return m
}

type Artist struct {
	Name       string
	Genres     []string
	Followers  uint
	Popularity int
}

type CurrentlyPlaying struct {
	PlayedAt   time.Time
	Artists    []Artist
	TrackName  string
	DurationMs int
}
