// Package botcore is the testable core of the chat example (design doc
// §11.4 #2): the JSON wire format shared by the browser page and the bot,
// the bot's !-command replies, and the post path (capped history list +
// publish) both sides use. The bot binary (examples/chat/bot) and the M7c
// end-to-end test (tests/m7_examples_test.go) drive this package.
package botcore

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pschlump/ultima/clients/go/ultima"
)

const (
	// Channel is the chat room's pub/sub topic.
	Channel = "chat:lobby"
	// HistKey is the room's message history, a list capped at HistCap.
	HistKey = "chat:lobby:hist"
	// HistCap bounds the history list (LTRIM -100 -1 after every post).
	HistCap = 100
	// PresencePrefix prefixes the per-user presence keys (SET with a short
	// PX, refreshed while the client is alive; expiry = user went away).
	PresencePrefix = "chat:presence:"
	// PresenceChannel carries join/heartbeat JSON {user} beats so live
	// clients can render the online list without polling.
	PresenceChannel = "chat:presence"
)

// Message is the chat payload on Channel and in HistKey.
type Message struct {
	User string `json:"user"`
	Text string `json:"text"`
	Ts   int64  `json:"ts"` // epoch milliseconds
}

// Exec is the command surface the bot needs; *ultima.WSClient and
// *ultima.GRPCClient both satisfy it.
type Exec interface {
	RPush(key string, elems ...string) (ultima.Value, error)
	Exec(cmd string, args ...string) (ultima.Value, error)
}

// Reply computes the bot's answer to a chat text; ok is false when the
// text is not a bot command.
func Reply(now time.Time, text string) (reply string, ok bool) {
	switch strings.TrimSpace(text) {
	case "!ping":
		return "PONG", true
	case "!time":
		return now.Format(time.RFC1123), true
	}
	return "", false
}

// Post appends m to the capped history and publishes it on Channel — the
// exact command sequence the browser page performs for user messages
// (RPUSH, LTRIM, PUBLISH).
func Post(e Exec, m Message) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if v, err := e.RPush(HistKey, string(payload)); err != nil {
		return err
	} else if ultima.IsError(v) {
		return fmt.Errorf("RPUSH %s: %s", HistKey, v.Str)
	}
	if v, err := e.Exec("LTRIM", HistKey, strconv.Itoa(-HistCap), "-1"); err != nil {
		return err
	} else if ultima.IsError(v) {
		return fmt.Errorf("LTRIM %s: %s", HistKey, v.Str)
	}
	if v, err := e.Exec("PUBLISH", Channel, string(payload)); err != nil {
		return err
	} else if ultima.IsError(v) {
		return fmt.Errorf("PUBLISH %s: %s", Channel, v.Str)
	}
	return nil
}

// Handle processes one Channel delivery: bot commands (!ping, !time) get a
// reply posted as user "bot"; anything else (and unparseable payloads) is
// ignored.
func Handle(e Exec, now time.Time, payload []byte) error {
	var m Message
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil
	}
	reply, ok := Reply(now, m.Text)
	if !ok || m.User == "bot" {
		return nil
	}
	return Post(e, Message{User: "bot", Text: reply, Ts: now.UnixMilli()})
}
