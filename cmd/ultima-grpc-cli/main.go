// ultima-grpc-cli is the gRPC operator CLI (design doc §6.4) against the
// binary surface (§6.2): one-shot commands run through the unary
// ExecGeneric RPC, the REPL pipelines over the bidi Exec stream, and
// SUBSCRIBE/PSUBSCRIBE/MONITOR open the dedicated push streams.
//
//	ultima-grpc-cli [-addr host:port] [-http-addr host:port] [--token tok | --user u --pass p [--totp t]] [command [arg ...]]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/pschlump/ultima/clients/go/ultima"
	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/resp"
)

func main() {
	os.Exit(run())
}

func run() int {
	fs := flag.NewFlagSet("ultima-grpc-cli", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:6380", "gRPC surface address")
	httpAddr := fs.String("http-addr", "127.0.0.1:6381", "HTTP surface address (used for --user login)")
	token := fs.String("token", "", "static JWT access token (§9.3)")
	user := fs.String("user", "", "management username (login against -http-addr)")
	pass := fs.String("pass", "", "management password")
	totp := fs.String("totp", "", "current TOTP code, when the account requires it")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return 1
	}
	args := fs.Args()

	var dialOpts []ultima.GRPCOption
	switch {
	case *token != "":
		dialOpts = append(dialOpts, ultima.WithGRPCToken(*token))
	case *user != "":
		tm := ultima.NewTokenManager(*httpAddr, ultima.Credentials{Username: *user, Password: *pass, TOTP: *totp})
		dialOpts = append(dialOpts, ultima.WithGRPCTokenFunc(tm.Token))
	}
	c, err := ultima.DialGRPC(*addr, dialOpts...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Could not connect to Ultima at", *addr+":", err)
		return 1
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	r := &ultima.Runner{
		Out:    os.Stdout,
		ErrOut: os.Stderr,
		Exec: func(args []string) (resp.Value, error) {
			return c.Exec(args[0], args[1:]...)
		},
		Stream: func(args []string) error { return streamGRPC(ctx, c, args) },
	}
	if len(args) > 0 {
		// One-shot: the unary ExecGeneric convenience RPC (§6.2).
		r.Exec = func(a []string) (resp.Value, error) {
			return c.ExecGeneric(ctx, a[0], a[1:]...)
		}
		return r.OneShot(strings.Join(args, " "))
	}
	if fi, err := os.Stdin.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
		return r.REPLReadline(fmt.Sprintf("ultima-grpc %s> ", *addr))
	}
	return r.REPL(os.Stdin, "")
}

// streamGRPC opens the Subscribe/Monitor server streams (§6.2) and prints
// events until Ctrl-C.
func streamGRPC(ctx context.Context, c *ultima.GRPCClient, args []string) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	verb := strings.ToUpper(args[0])
	if verb == "MONITOR" {
		err := c.Monitor(ctx, func(ev *ultimav1.CommandEvent) {
			var sb strings.Builder
			fmt.Fprintf(&sb, "+%.6f [%d %s]", float64(ev.GetUnixMs())/1000, ev.GetDb(), ev.GetClientAddr())
			fmt.Fprintf(&sb, " %q", ev.GetCommand())
			for _, a := range ev.GetArgs() {
				fmt.Fprintf(&sb, " %q", string(a))
			}
			_, _ = fmt.Fprintln(os.Stdout, sb.String())
		})
		return streamErr(err)
	}
	var channels, patterns []string
	if verb == "PSUBSCRIBE" {
		patterns = args[1:]
	} else {
		channels = args[1:]
	}
	err := c.Subscribe(ctx, channels, patterns, func(ev *ultimav1.PushEvent) {
		ultima.Fprint(os.Stdout, pushValueOf(ev))
	})
	return streamErr(err)
}

// streamErr maps the expected Ctrl-C cancellation of a push stream to the
// interrupted sentinel (clean exit 0).
func streamErr(err error) error {
	if err != nil && strings.Contains(err.Error(), "context canceled") {
		return ultima.Interrupted()
	}
	return err
}

// pushValueOf renders a PushEvent as the RESP push frame redis-cli would
// print: acks as [subscribe channel count], deliveries as
// [message channel payload] / [pmessage pattern channel payload].
func pushValueOf(ev *ultimav1.PushEvent) resp.Value {
	if ev.GetIsAck() {
		kind := "subscribe"
		if ev.GetPattern() != "" {
			kind = "psubscribe"
		}
		return resp.Push(resp.BlobStr(kind), resp.BlobStr(ev.GetChannel()), resp.Int(ev.GetCount()))
	}
	if ev.GetPattern() != "" {
		return resp.Push(resp.BlobStr("pmessage"), resp.BlobStr(ev.GetPattern()),
			resp.BlobStr(ev.GetChannel()), resp.BlobString(ev.GetPayload()))
	}
	return resp.Push(resp.BlobStr("message"), resp.BlobStr(ev.GetChannel()), resp.BlobString(ev.GetPayload()))
}
