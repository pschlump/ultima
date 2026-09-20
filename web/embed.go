// Package web serves the M6d management UI (design doc §10.2): the
// React+vite app under web/ builds to web/dist (`make web`), which is
// embedded here and served from the HTTP port with SPA fallback. The
// committed placeholder dist keeps `make build`/`make test` working on a
// checkout where the UI was never built.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var dist embed.FS

// Dist returns the embedded UI build rooted at dist/.
func Dist() fs.FS {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err) // the embed pattern guarantees dist exists
	}
	return sub
}
