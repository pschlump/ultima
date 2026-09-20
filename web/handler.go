package web

import (
	"io/fs"
	"net/http"
	"strings"
)

// Handler serves the embedded UI (§10.2) as a single-page app: real
// files come straight from the embedded dist, every other non-API path
// falls back to index.html so client-side routes (/console, /keys, …)
// survive reloads. Paths belonging to the machine surfaces that arrive
// here (i.e. matched no API route) are a plain 404 — the SPA must never
// swallow an API typo with an HTML page. index.html is no-cache so a
// redeployed UI is picked up; vite's hashed /assets are immutable.
func Handler() http.Handler {
	return &spaHandler{files: http.FS(Dist())}
}

type spaHandler struct {
	files http.FileSystem
}

// machinePrefixes are the path spaces the SPA fallback must not answer
// for: anything under them that reached this handler matched no real
// route and is a genuine 404.
var machinePrefixes = []string{"/api/", "/ws/", "/metrics", "/health", "/ready"}

func (h *spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	for _, prefix := range machinePrefixes {
		if strings.HasPrefix(p, prefix) {
			http.NotFound(w, r)
			return
		}
	}
	name := strings.TrimPrefix(p, "/")
	if name == "" {
		name = "index.html"
	}
	f, err := h.files.Open(name)
	if err != nil {
		serveIndex(w, r)
		return
	}
	_ = f.Close()
	if strings.HasPrefix(p, "/assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else if name == "index.html" {
		w.Header().Set("Cache-Control", "no-cache")
	}
	http.FileServer(h.files).ServeHTTP(w, r)
}

// serveIndex replies with the SPA shell for a client-side route.
func serveIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-cache")
	b, err := fs.ReadFile(Dist(), "index.html")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}
