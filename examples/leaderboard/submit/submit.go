// Package submit is the testable core of the leaderboard example's score
// submitter (design doc §11.4 #1): each submission ZADDs a score into the
// lb:scores sorted set and PUBLISHes a JSON {player, score} update on
// lb:update so live pages can refresh. The submitter binary
// (examples/leaderboard/submitter) and the M7c end-to-end test
// (tests/m7_examples_test.go) both drive this package.
package submit

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/pschlump/ultima/clients/go/ultima"
)

const (
	// ScoresKey is the sorted set holding the leaderboard (member = player,
	// score = latest submitted score).
	ScoresKey = "lb:scores"
	// UpdateChannel carries one JSON Update payload per submission.
	UpdateChannel = "lb:update"
)

// Exec is the command surface the submitter needs; *ultima.WSClient and
// *ultima.GRPCClient both satisfy it.
type Exec interface {
	ZAdd(key string, members ...ultima.ZMember) (ultima.Value, error)
	Exec(cmd string, args ...string) (ultima.Value, error)
}

// Update is the lb:update payload: one score submission.
type Update struct {
	Player string  `json:"player"`
	Score  float64 `json:"score"`
}

// Config tunes Run.
type Config struct {
	Rate    float64 // submissions per second (default 5)
	Players int     // simulated player pool, player-1 .. player-N (default 20)
}

func (c Config) withDefaults() Config {
	if c.Rate <= 0 {
		c.Rate = 5
	}
	if c.Players <= 0 {
		c.Players = 20
	}
	return c
}

// One ZADDs a random score (0..9999) for a random player and publishes the
// update; it returns exactly what was submitted.
func One(e Exec, rnd *rand.Rand, players int) (Update, error) {
	if players <= 0 {
		players = 20
	}
	u := Update{
		Player: "player-" + strconv.Itoa(rnd.Intn(players)+1),
		Score:  float64(rnd.Intn(10000)),
	}
	if v, err := e.ZAdd(ScoresKey, ultima.ZMember{Score: u.Score, Member: u.Player}); err != nil {
		return u, err
	} else if ultima.IsError(v) {
		return u, fmt.Errorf("ZADD %s: %s", ScoresKey, v.Str)
	}
	payload, err := json.Marshal(u)
	if err != nil {
		return u, err
	}
	if v, err := e.Exec("PUBLISH", UpdateChannel, string(payload)); err != nil {
		return u, err
	} else if ultima.IsError(v) {
		return u, fmt.Errorf("PUBLISH %s: %s", UpdateChannel, v.Str)
	}
	return u, nil
}

// Run submits scores at cfg.Rate until ctx is cancelled; submission errors
// are logged and the loop continues (a reconnecting client surfaces
// transient failures here).
func Run(ctx context.Context, e Exec, cfg Config, rnd *rand.Rand, logf func(format string, args ...any)) error {
	cfg = cfg.withDefaults()
	ticker := time.NewTicker(time.Duration(float64(time.Second) / cfg.Rate))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			u, err := One(e, rnd, cfg.Players)
			if err != nil {
				logf("submit: %s", err)
				continue
			}
			logf("%s scores %.0f", u.Player, u.Score)
		}
	}
}
