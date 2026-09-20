// Package ultima REPL machinery for the three CLIs (§6.4): a shellish
// line splitter, the streaming-verb check, and the one-shot/REPL drivers
// that render replies via render.go. Kept in the client library so the
// cmd/ mains stay thin shells.
package ultima

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pschlump/ultima/lib/resp"
)

// Runner executes parsed command lines against one surface and renders
// the replies redis-cli-style.
type Runner struct {
	Out    io.Writer // reply rendering
	ErrOut io.Writer // transport errors and one-shot error replies
	// Exec runs one command; error replies arrive as KindError values.
	Exec func(args []string) (resp.Value, error)
	// Stream runs SUBSCRIBE/PSUBSCRIBE/MONITOR: it blocks, printing push
	// frames, until interrupted (Ctrl-C) — the CLIs arrange the signal
	// handling that makes Stream return.
	Stream func(args []string) error
}

// OneShot runs one command line (the CLI's positional args, joined) and
// returns the process exit code: 1 on a transport failure or an error
// reply (both to stderr), 0 otherwise. Streaming verbs enter streaming
// mode.
func (r *Runner) OneShot(line string) int {
	args, err := SplitShell(line)
	if err != nil {
		_, _ = fmt.Fprintln(r.ErrOut, err)
		return 1
	}
	if len(args) == 0 {
		return 0
	}
	if StreamingVerb(args) {
		if err := r.Stream(args); err != nil && !errors.Is(err, errInterrupted) {
			_, _ = fmt.Fprintln(r.ErrOut, err)
			return 1
		}
		return 0
	}
	v, err := r.Exec(args)
	if err != nil {
		_, _ = fmt.Fprintln(r.ErrOut, err)
		return 1
	}
	if v.Kind == resp.KindError {
		_, _ = fmt.Fprintln(r.ErrOut, Format(v))
		return 1
	}
	Fprint(r.Out, v)
	return 0
}

// REPL reads lines from in until EOF or a quit/exit line, executing each
// non-empty line; prompt is printed before each read when non-empty (the
// CLIs pass "" when stdin is not a terminal). Unlike one-shot mode,
// error replies print inline (to Out) and do not end the loop.
func (r *Runner) REPL(in io.Reader, prompt string) int {
	sc := bufio.NewScanner(in)
	for {
		if prompt != "" {
			_, _ = fmt.Fprint(r.Out, prompt)
		}
		if !sc.Scan() {
			return 0 // EOF (piped input done) or read error
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		args, err := SplitShell(line)
		if err != nil {
			_, _ = fmt.Fprintln(r.Out, err)
			continue
		}
		if len(args) == 0 {
			continue
		}
		if isQuit(args[0]) {
			return 0
		}
		if StreamingVerb(args) {
			if err := r.Stream(args); err != nil && !errors.Is(err, errInterrupted) {
				_, _ = fmt.Fprintln(r.ErrOut, err)
			}
			continue
		}
		v, err := r.Exec(args)
		if err != nil {
			_, _ = fmt.Fprintln(r.ErrOut, err)
			continue
		}
		Fprint(r.Out, v)
	}
}

// errInterrupted marks a Stream return caused by the CLI's own Ctrl-C
// handling (a clean exit, not a failure).
var errInterrupted = errors.New("ultima: interrupted")

// Interrupted is the sentinel Stream implementations should return (or
// wrap) when the user ended streaming mode with Ctrl-C.
func Interrupted() error { return errInterrupted }

func isQuit(verb string) bool {
	return strings.EqualFold(verb, "quit") || strings.EqualFold(verb, "exit")
}

// StreamingVerb reports whether args invokes a streaming command —
// SUBSCRIBE/PSUBSCRIBE/MONITOR put the CLI in streaming mode (§6.4).
func StreamingVerb(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch strings.ToUpper(args[0]) {
	case "SUBSCRIBE", "PSUBSCRIBE", "MONITOR":
		return true
	}
	return false
}

// AnyArgs adapts split string args for RESPClient.Exec's ...any form.
func AnyArgs(args []string) []any {
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = a
	}
	return out
}

// SplitShell splits one command line into arguments: whitespace
// separates, single and double quotes group (double quotes honor the C
// escapes \n \r \t \xNN \" \\), and a backslash escapes the next
// character outside quotes — the redis-cli input grammar.
func SplitShell(line string) ([]string, error) {
	var args []string
	var cur strings.Builder
	inArg := false
	flush := func() {
		if inArg {
			args = append(args, cur.String())
			cur.Reset()
			inArg = false
		}
	}
	runes := []rune(line)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n':
			flush()
		case c == '\\' && i+1 < len(runes):
			i++
			cur.WriteRune(runes[i])
			inArg = true
		case c == '\'':
			inArg = true
			for i+1 < len(runes) && runes[i+1] != '\'' {
				i++
				cur.WriteRune(runes[i])
			}
			if i+1 >= len(runes) {
				return nil, errors.New("ultima: unterminated single quote")
			}
			i++
		case c == '"':
			inArg = true
			closed := false
			for i+1 < len(runes) {
				i++
				d := runes[i]
				if d == '"' {
					closed = true
					break
				}
				if d == '\\' && i+1 < len(runes) {
					i++
					switch e := runes[i]; e {
					case 'n':
						cur.WriteByte('\n')
					case 'r':
						cur.WriteByte('\r')
					case 't':
						cur.WriteByte('\t')
					case 'b':
						cur.WriteByte('\b')
					case 'a':
						cur.WriteByte('\a')
					case 'x':
						if i+2 < len(runes) {
							if v, err := strconv.ParseUint(string(runes[i+1:i+3]), 16, 8); err == nil {
								cur.WriteByte(byte(v))
								i += 2
								continue
							}
						}
						cur.WriteByte('x')
					default:
						cur.WriteRune(e)
					}
					continue
				}
				cur.WriteRune(d)
			}
			if !closed {
				return nil, errors.New("ultima: unterminated double quote")
			}
		default:
			cur.WriteRune(c)
			inArg = true
		}
	}
	flush()
	return args, nil
}
