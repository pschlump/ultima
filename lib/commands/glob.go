package commands

// GlobMatch is a port of Redis's stringmatchlen (util.c): '*', '?',
// bracket classes ([abc], [^abc], [a-z]), and '\' escapes; case
// sensitive. Used by SCAN MATCH and CONFIG GET patterns.
func GlobMatch(pattern, s []byte) bool {
	p, str := pattern, s
	for len(p) > 0 {
		switch p[0] {
		case '*':
			for len(p) > 1 && p[1] == '*' { // collapse consecutive stars
				p = p[1:]
			}
			if len(p) == 1 {
				return true // trailing star matches everything
			}
			// try to match the rest at every position
			for i := 0; i <= len(str); i++ {
				if GlobMatch(p[1:], str[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(str) == 0 {
				return false
			}
			str = str[1:]
		case '[':
			p = p[1:]
			negate := len(p) > 0 && p[0] == '^'
			if negate {
				p = p[1:]
			}
			matched := false
			for {
				if len(p) == 0 {
					return false // unterminated class
				}
				if p[0] == '\\' {
					if len(p) < 2 {
						return false
					}
					if len(str) > 0 && p[1] == str[0] {
						matched = true
					}
					p = p[2:]
				} else if p[0] == ']' {
					break
				} else if len(p) >= 3 && p[1] == '-' {
					// range: p[0]-p[2]
					lo, hi := p[0], p[2]
					if lo > hi {
						lo, hi = hi, lo
					}
					if len(str) > 0 && str[0] >= lo && str[0] <= hi {
						matched = true
					}
					p = p[3:]
				} else {
					if len(str) > 0 && p[0] == str[0] {
						matched = true
					}
					p = p[1:]
				}
			}
			if matched == negate {
				return false
			}
			if len(str) == 0 {
				return false
			}
			str = str[1:]
		case '\\':
			if len(p) >= 2 {
				p = p[1:]
			}
			fallthrough
		default:
			if len(str) == 0 || p[0] != str[0] {
				return false
			}
			str = str[1:]
		}
		p = p[1:]
	}
	return len(str) == 0
}
