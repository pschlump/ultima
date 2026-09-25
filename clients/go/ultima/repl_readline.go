// Interactive REPL for the three CLIs (§6.4) built on
// github.com/ergochat/readline (the vendored fork in third_party/readline):
// line editing, in-memory history, tab completion of command names (and
// of `help` arguments), and a switchable vi/emacs editing mode via the
// client-side `\mode` meta-command. Used when stdin is a terminal; piped
// input keeps the plain scanner REPL in repl.go.

package ultima

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/ergochat/readline"
)

// REPLReadline runs the interactive REPL with readline line editing.
// Ctrl-C cancels the current input line, Ctrl-D (EOF) or quit/exit ends
// the loop. `\mode vi` / `\mode emacs` switch the editing mode (bare
// `\mode` reports the current one); `\mode` lines are handled
// client-side and never reach the server.
func (r *Runner) REPLReadline(prompt string) int {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:       prompt,
		HistoryLimit: 1000,
		AutoComplete: cmdCompleter{},
	})
	if err != nil {
		_, _ = fmt.Fprintln(r.ErrOut, "ultima: readline unavailable:", err)
		return 1
	}
	defer func() { _ = rl.Close() }()

	for {
		line, err := rl.Readline()
		if errors.Is(err, readline.ErrInterrupt) {
			continue // Ctrl-C: abandon the current line
		}
		if errors.Is(err, io.EOF) {
			return 0 // Ctrl-D
		}
		if err != nil {
			_, _ = fmt.Fprintln(r.ErrOut, err)
			return 1
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if r.editModeCommand(rl, line) {
			continue
		}
		args, err := SplitShell(line)
		if err != nil {
			_, _ = fmt.Fprintln(r.Out, err)
			continue
		}
		if r.runLine(args) {
			return 0
		}
	}
}

// editModeCommand handles the client-side `\mode [vi|emacs]` meta-
// command against rl's raw input line (checked before SplitShell, which
// would consume the backslash); it reports whether line was one.
func (r *Runner) editModeCommand(rl *readline.Instance, line string) bool {
	rest, ok := strings.CutPrefix(line, `\mode`)
	if !ok || (rest != "" && rest[0] != ' ' && rest[0] != '\t') {
		return false
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		mode := "emacs"
		if rl.IsVimMode() {
			mode = "vi"
		}
		_, _ = fmt.Fprintln(r.Out, "editing mode:", mode)
		return true
	}
	switch strings.ToLower(fields[0]) {
	case "vi", "vim":
		rl.SetVimMode(true)
		_, _ = fmt.Fprintln(r.Out, "editing mode: vi")
	case "emacs":
		rl.SetVimMode(false)
		_, _ = fmt.Fprintln(r.Out, "editing mode: emacs")
	default:
		_, _ = fmt.Fprintln(r.Out, `usage: \mode [vi|emacs]`)
	}
	return true
}
