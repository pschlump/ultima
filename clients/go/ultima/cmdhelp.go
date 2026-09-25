// Static command help for the CLIs (§6.4): `help` lists the supported
// commands by group, `help @<group>` one group, and `help <command>`
// the details (syntax, summary, since, group) of one command. The data
// table is cmdhelp_data.go, generated from lib/commands/table.go plus
// the Redis 7.2.7 command metadata (bin/gen-cli-help.py), so it matches
// the engine's command set and the compat target's documentation.
// Handled client-side like redis-cli; it never reaches the server.

package ultima

import (
	"fmt"
	"sort"
	"strings"
)

// cmdHelpEntry is one command's static help record (see cmdhelp_data.go).
type cmdHelpEntry struct {
	Name    string // lowercase, as typed
	Group   string // engine group (lib/commands/table.go)
	Syntax  string // e.g. "SET key value [NX | XX] ..."
	Summary string
	Since   string // Redis version that introduced the command
}

// cmdHelpLookup finds the entry for name (case-insensitive).
func cmdHelpLookup(name string) *cmdHelpEntry {
	name = strings.ToLower(name)
	i := sort.Search(len(cmdHelpEntries), func(i int) bool {
		return cmdHelpEntries[i].Name >= name
	})
	if i < len(cmdHelpEntries) && cmdHelpEntries[i].Name == name {
		return &cmdHelpEntries[i]
	}
	return nil
}

// cmdHelpGroups returns the distinct group names, sorted.
func cmdHelpGroups() []string {
	seen := map[string]bool{}
	var groups []string
	for _, e := range cmdHelpEntries {
		if !seen[e.Group] {
			seen[e.Group] = true
			groups = append(groups, e.Group)
		}
	}
	sort.Strings(groups)
	return groups
}

// printHelp implements the client-side `help` command: bare `help`
// lists every command by group, `help @<group>` one group, and
// `help <command>` one command's details.
func (r *Runner) printHelp(args []string) {
	w := r.Out
	if len(args) == 0 {
		_, _ = fmt.Fprintln(w, `Use "help <command>" for details on a command, "help @<group>" to list`)
		_, _ = fmt.Fprintln(w, `the commands in a group, and "\mode vi|emacs" to switch editing mode.`)
		_, _ = fmt.Fprintln(w)
		for _, g := range cmdHelpGroups() {
			var names []string
			for _, e := range cmdHelpEntries {
				if e.Group == g {
					names = append(names, e.Name)
				}
			}
			_, _ = fmt.Fprintf(w, "  @%s\n    %s\n", g, strings.Join(names, " "))
		}
		return
	}
	if strings.HasPrefix(args[0], "@") {
		g := strings.ToLower(strings.TrimPrefix(args[0], "@"))
		var names []string
		for _, e := range cmdHelpEntries {
			if e.Group == g {
				names = append(names, e.Name)
			}
		}
		if len(names) == 0 {
			_, _ = fmt.Fprintf(w, "unknown group '@%s' (try \"help\" for the group list)\n", g)
			return
		}
		_, _ = fmt.Fprintf(w, "  @%s\n    %s\n", g, strings.Join(names, " "))
		return
	}
	e := cmdHelpLookup(args[0])
	if e == nil {
		_, _ = fmt.Fprintf(w, "unknown command '%s' (try \"help\" for the command list)\n", strings.ToLower(args[0]))
		return
	}
	_, _ = fmt.Fprintln(w, " ", e.Syntax)
	if e.Summary != "" {
		_, _ = fmt.Fprintln(w, "  summary:", e.Summary)
	}
	if e.Since != "" {
		_, _ = fmt.Fprintln(w, "  since:", e.Since)
	}
	_, _ = fmt.Fprintln(w, "  group:", e.Group)
}

// isHelpVerb reports whether the first word of a REPL line is the
// client-side help command.
func isHelpVerb(verb string) bool {
	return strings.EqualFold(verb, "help")
}

// cmdCompleter is a readline.AutoCompleter over the command table: the
// first word of a line completes against command names (plus help and
// exit), and the second word of a `help` line against command names and
// @groups. Matching is case-insensitive; completions are the lowercase
// canonical name with a trailing space.
type cmdCompleter struct{}

// Do implements readline.AutoCompleter.
func (cmdCompleter) Do(line []rune, pos int) ([][]rune, int) {
	if pos > len(line) {
		pos = len(line)
	}
	start := pos
	for start > 0 && line[start-1] != ' ' && line[start-1] != '\t' {
		start--
	}
	before := strings.Fields(string(line[:start]))
	var cands []string
	switch {
	case len(before) == 0:
		for _, e := range cmdHelpEntries {
			cands = append(cands, e.Name)
		}
		cands = append(cands, "exit", "help")
	case len(before) == 1 && strings.EqualFold(before[0], "help"):
		for _, e := range cmdHelpEntries {
			cands = append(cands, e.Name)
		}
		for _, g := range cmdHelpGroups() {
			cands = append(cands, "@"+g)
		}
	default:
		return nil, 0
	}
	prefix := strings.ToLower(string(line[start:pos]))
	var out [][]rune
	for _, c := range cands {
		if strings.HasPrefix(c, prefix) {
			out = append(out, []rune(c[len(prefix):]+" "))
		}
	}
	return out, pos - start
}
