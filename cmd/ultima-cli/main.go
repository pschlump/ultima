// ultima-cli is the RESP operator CLI (design doc §6.4): the redis-cli
// analogue against the RESP surface. One-shot mode runs the positional
// args as one command; with no args it is a line-oriented REPL (no
// readline dependency). SUBSCRIBE/PSUBSCRIBE/MONITOR enter streaming
// mode, printing pushes until Ctrl-C.
//
//	ultima-cli [-h host] [-p port] [-a password] [-n db] [-3] [command [arg ...]]
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/pschlump/ultima/clients/go/ultima"
	"github.com/pschlump/ultima/lib/resp"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(argv []string) int {
	var host, port, password string
	var db int
	var resp3 bool
	var args []string
	// Hand-rolled flag scan (stdlib flag would exit(2) on -h, colliding
	// with redis-cli's host flag). Like redis-cli, flag parsing stops at
	// the first positional argument: everything after the command name is
	// command args, even when it looks like a flag (`ZRANGE k -3 -1`).
	for i := 0; i < len(argv); i++ {
		if len(args) > 0 {
			args = append(args, argv[i])
			continue
		}
		switch argv[i] {
		case "-h":
			i++
			if i >= len(argv) {
				fmt.Fprintln(os.Stderr, "ultima-cli: -h requires an argument")
				return 1
			}
			host = argv[i]
		case "-p":
			i++
			if i >= len(argv) {
				fmt.Fprintln(os.Stderr, "ultima-cli: -p requires an argument")
				return 1
			}
			port = argv[i]
		case "-a":
			i++
			if i >= len(argv) {
				fmt.Fprintln(os.Stderr, "ultima-cli: -a requires an argument")
				return 1
			}
			password = argv[i]
		case "-n":
			i++
			if i >= len(argv) {
				fmt.Fprintln(os.Stderr, "ultima-cli: -n requires an argument")
				return 1
			}
			if _, err := fmt.Sscanf(argv[i], "%d", &db); err != nil {
				fmt.Fprintln(os.Stderr, "ultima-cli: invalid -n db:", argv[i])
				return 1
			}
		case "-3":
			resp3 = true
		case "--help":
			fmt.Fprintln(os.Stderr, "usage: ultima-cli [-h host] [-p port] [-a password] [-n db] [-3] [command [arg ...]]")
			return 0
		default:
			args = append(args, argv[i])
		}
	}
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "6379"
	}

	opts := []ultima.RESPOption{ultima.WithRESPPassword(password), ultima.WithRESPDB(db)}
	if resp3 {
		opts = append(opts, ultima.WithRESPProto(3))
	}
	c, err := ultima.DialRESP(host+":"+port, opts...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Could not connect to Ultima at", host+":"+port+":", err)
		return 1
	}
	defer func() { _ = c.Close() }()

	r := &ultima.Runner{
		Out:    os.Stdout,
		ErrOut: os.Stderr,
		Exec: func(args []string) (resp.Value, error) {
			return c.Exec(ultima.AnyArgs(args)...)
		},
		Stream: func(args []string) error {
			return streamRESP(c, args)
		},
	}
	if len(args) > 0 {
		return r.OneShot(strings.Join(args, " "))
	}
	prompt := ""
	if isTerminal() {
		prompt = fmt.Sprintf("ultima %s:%s> ", host, port)
	}
	return r.REPL(os.Stdin, prompt)
}

// streamRESP runs SUBSCRIBE/PSUBSCRIBE/MONITOR and prints every frame
// (acks included) until Ctrl-C.
func streamRESP(c *ultima.RESPClient, args []string) error {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	defer signal.Stop(sig)
	done := make(chan error, 1)
	go func() {
		done <- c.Stream(func(v resp.Value) {
			ultima.Fprint(os.Stdout, v)
		}, ultima.AnyArgs(args)...)
	}()
	select {
	case <-sig:
		_ = c.Close() // unblocks Stream
		<-done
		return ultima.Interrupted()
	case err := <-done:
		return err
	}
}

// isTerminal reports whether stdin is an interactive terminal (prompts
// and piped-input cleanliness depend on it).
func isTerminal() bool {
	fi, err := os.Stdin.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
