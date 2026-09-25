// ultima-ws-cli is the WebSocket operator CLI (design doc §6.4) against
// /ws/v1 (§6.3): same one-shot/REPL/streaming behavior as ultima-cli,
// with §9.4 resumable sessions by default (drops reconnect and replay
// missed pushes transparently) and JWT auth via --token or
// --user/--pass/--totp (§9.3).
//
//	ultima-ws-cli [-addr host:port] [--token tok | --user u --pass p [--totp t]] [--no-session] [command [arg ...]]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/pschlump/ultima/clients/go/ultima"
	"github.com/pschlump/ultima/lib/resp"
)

func main() {
	os.Exit(run())
}

func run() int {
	fs := flag.NewFlagSet("ultima-ws-cli", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:6381", "HTTP/WS surface address")
	token := fs.String("token", "", "static JWT access token (§9.3)")
	user := fs.String("user", "", "management username (login against -addr)")
	pass := fs.String("pass", "", "management password")
	totp := fs.String("totp", "", "current TOTP code, when the account requires it")
	noSession := fs.Bool("no-session", false, "disable §9.4 resumable sessions")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return 1
	}
	args := fs.Args()

	opts := ultima.WSOptions{
		Addr:            *addr,
		DisableSessions: *noSession,
		// Streaming mode prints pushes; the banner marks reconnects.
		OnPush: func(v resp.Value, _ uint64) { ultima.Fprint(os.Stdout, v) },
		OnGap: func() {
			fmt.Fprintln(os.Stderr, "ultima-ws-cli: session lost (SESSION_EXPIRED); re-subscribed, pushes may have been missed")
		},
		OnResume: func() { fmt.Fprintln(os.Stderr, "ultima-ws-cli: session resumed (no gap)") },
	}
	switch {
	case *token != "":
		opts.TokenProvider = func(context.Context) (string, error) { return *token, nil }
	case *user != "":
		tm := ultima.NewTokenManager(*addr, ultima.Credentials{Username: *user, Password: *pass, TOTP: *totp})
		opts.TokenProvider = tm.Token
	}

	c, err := ultima.DialWS(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Could not connect to Ultima at", *addr+":", err)
		return 1
	}
	defer func() { _ = c.Close() }()

	r := &ultima.Runner{
		Out:    os.Stdout,
		ErrOut: os.Stderr,
		Exec: func(args []string) (resp.Value, error) {
			return c.Exec(args[0], args[1:]...)
		},
		Stream: func(args []string) error { return streamWS(c, args) },
	}
	if len(args) > 0 {
		return r.OneShot(strings.Join(args, " "))
	}
	if fi, err := os.Stdin.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
		return r.REPLReadline(fmt.Sprintf("ultima-ws %s> ", *addr))
	}
	return r.REPL(os.Stdin, "")
}

// streamWS runs SUBSCRIBE/PSUBSCRIBE (tracked by the client, so §9.4
// recovery re-subscribes after reconnects) or MONITOR, then blocks
// printing pushes (via OnPush) until Ctrl-C.
func streamWS(c *ultima.WSClient, args []string) error {
	var v resp.Value
	var err error
	switch strings.ToUpper(args[0]) {
	case "SUBSCRIBE":
		v, err = c.Subscribe(args[1:]...)
	case "PSUBSCRIBE":
		v, err = c.PSubscribe(args[1:]...)
	case "MONITOR":
		v, err = c.Exec("MONITOR")
	}
	if err != nil {
		return err
	}
	if v.Kind == resp.KindError {
		// Error replies print like any other reply; streaming just never starts.
		ultima.Fprint(os.Stdout, v)
		return ultima.Interrupted()
	}
	ultima.Fprint(os.Stdout, v) // first subscribe ack
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	signal.Stop(sig)
	return ultima.Interrupted()
}
