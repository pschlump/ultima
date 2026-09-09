package httpapi

// Server-introspection and persistence handlers (§10.1): /health, /ready,
// /api/v1/ping, /info, /shards, /clients (+kill), /slowlog, /latency,
// /config GET|PUT, /flushdb, and the /save family. Engine access goes
// through commands.Engine.Execute on a synthetic ConnState (D3) or the
// M6c introspection methods (lib/commands/clients.go, slowlog.go).

import (
	"net/http"
	"sort"
	"strings"

	httpapigen "github.com/pschlump/ultima/gen/httpapi"
	"github.com/pschlump/ultima/lib/resp"
)

// GetHealth implements GET /health (public liveness probe).
func (s *Server) GetHealth(w http.ResponseWriter, _ *http.Request) {
	statusOK(w)
}

// GetReady implements GET /ready (public readiness probe).
func (s *Server) GetReady(w http.ResponseWriter, _ *http.Request) {
	statusOK(w)
}

// GetPing implements GET /api/v1/ping.
func (s *Server) GetPing(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, httpapigen.Ping{Message: "PONG"})
}

// exec runs one engine command on a synthetic connection for db.
func (s *Server) exec(r *http.Request, db int, args ...string) resp.Value {
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	return s.eng.Execute(s.syntheticConn(r, db), argv)
}

// valStr renders a string-carrying reply value (blob, simple, verbatim,
// error) as a Go string.
func valStr(v resp.Value) string {
	if v.Kind == resp.KindBlobString || v.Kind == resp.KindVerbatim {
		return string(v.Blob)
	}
	return v.Str
}

// GetInfo implements GET /api/v1/info: INFO's bulk string parsed into
// sections → fields; values stay strings, exactly as on the RESP wire.
func (s *Server) GetInfo(w http.ResponseWriter, r *http.Request) {
	v := s.exec(r, 0, "INFO")
	if v.Kind == resp.KindError {
		writeError(w, http.StatusInternalServerError, v.Str)
		return
	}
	sections := map[string]map[string]string{}
	cur := ""
	for _, line := range strings.Split(string(v.Blob), "\n") {
		line = strings.TrimRight(line, "\r")
		switch {
		case line == "":
		case strings.HasPrefix(line, "# "):
			cur = strings.ToLower(strings.TrimPrefix(line, "# "))
			sections[cur] = map[string]string{}
		default:
			name, value, ok := strings.Cut(line, ":")
			if ok && cur != "" {
				sections[cur][name] = value
			}
		}
	}
	writeJSON(w, http.StatusOK, httpapigen.Info{Sections: sections})
}

// GetShards implements GET /api/v1/shards.
func (s *Server) GetShards(w http.ResponseWriter, _ *http.Request) {
	stats := s.eng.Shards.ShardStats()
	out := make([]httpapigen.ShardStat, 0, len(stats))
	for _, st := range stats {
		out = append(out, httpapigen.ShardStat{
			Index:      st.Index,
			Keys:       st.Keys,
			Expires:    st.Expires,
			MemBytes:   st.MemBytes,
			QueueDepth: st.QueueDepth,
			ExpiryHeap: st.ExpiryHeap,
		})
	}
	writeJSON(w, http.StatusOK, httpapigen.ShardList{Shards: out})
}

// GetClients implements GET /api/v1/clients.
func (s *Server) GetClients(w http.ResponseWriter, _ *http.Request) {
	clients := s.eng.ListClients()
	out := make([]httpapigen.ClientInfo, 0, len(clients))
	for _, c := range clients {
		out = append(out, httpapigen.ClientInfo{
			Id:            c.ID,
			Addr:          c.Addr,
			Name:          c.Name,
			User:          c.User,
			Db:            c.DB,
			Proto:         c.Proto,
			Surface:       httpapigen.ClientInfoSurface(c.Surface),
			AgeSeconds:    c.AgeSeconds,
			IdleSeconds:   c.IdleSeconds,
			LastCommand:   c.LastCommand,
			Subscriptions: c.Subscriptions,
			Patterns:      c.Patterns,
		})
	}
	writeJSON(w, http.StatusOK, httpapigen.ClientList{Clients: out})
}

// KillClient implements POST /api/v1/clients/{id}/kill.
func (s *Server) KillClient(w http.ResponseWriter, _ *http.Request, id uint64) {
	found, killable := s.eng.KillClient(id)
	switch {
	case !found:
		writeError(w, http.StatusNotFound, "not_found")
	case !killable:
		writeError(w, http.StatusConflict, "client cannot be killed")
	default:
		statusOK(w)
	}
}

// GetSlowlog implements GET /api/v1/slowlog; count <= 0 (absent) returns
// the whole ring.
func (s *Server) GetSlowlog(w http.ResponseWriter, _ *http.Request, params httpapigen.GetSlowlogParams) {
	count := 0
	if params.Count != nil {
		count = *params.Count
	}
	entries := s.eng.Slowlog(count)
	out := make([]httpapigen.SlowlogEntry, 0, len(entries))
	for _, e := range entries {
		args := e.Args
		if args == nil {
			args = []string{}
		}
		out = append(out, httpapigen.SlowlogEntry{
			Id:         e.ID,
			Timestamp:  e.Timestamp,
			DurationUs: e.DurationUs,
			Args:       args,
			ClientAddr: e.ClientAddr,
			ClientName: e.ClientName,
		})
	}
	writeJSON(w, http.StatusOK, httpapigen.SlowlogList{Entries: out})
}

// ResetSlowlog implements DELETE /api/v1/slowlog (SLOWLOG RESET).
func (s *Server) ResetSlowlog(w http.ResponseWriter, _ *http.Request) {
	s.eng.SlowlogReset()
	statusOK(w)
}

// GetLatency implements GET /api/v1/latency.
func (s *Server) GetLatency(w http.ResponseWriter, _ *http.Request) {
	stats := s.eng.LatencyStats()
	out := make([]httpapigen.CommandLatency, 0, len(stats))
	for _, st := range stats {
		out = append(out, httpapigen.CommandLatency{
			Command: st.Command,
			Count:   st.Count,
			TotalUs: st.TotalUs,
			MaxUs:   st.MaxUs,
			AvgUs:   st.AvgUs,
		})
	}
	writeJSON(w, http.StatusOK, httpapigen.LatencyStats{Commands: out})
}

// GetConfig implements GET /api/v1/config (CONFIG GET *): the reply map's
// flattened pairs become the entries object.
func (s *Server) GetConfig(w http.ResponseWriter, r *http.Request) {
	v := s.exec(r, 0, "CONFIG", "GET", "*")
	if v.Kind == resp.KindError {
		writeError(w, http.StatusInternalServerError, v.Str)
		return
	}
	entries := map[string]string{}
	for i := 0; i+1 < len(v.Arr); i += 2 {
		entries[valStr(v.Arr[i])] = valStr(v.Arr[i+1])
	}
	writeJSON(w, http.StatusOK, httpapigen.ConfigMap{Entries: entries})
}

// PutConfig implements PUT /api/v1/config: each entry is applied via
// CONFIG SET in sorted key order (deterministic first failure); a Redis
// error reply comes back verbatim in the 400 body.
func (s *Server) PutConfig(w http.ResponseWriter, r *http.Request) {
	var body httpapigen.ConfigUpdateRequest
	if !JsonBody(w, r, &body) {
		return
	}
	if len(body.Entries) == 0 {
		writeError(w, http.StatusBadRequest, "entries must not be empty")
		return
	}
	names := make([]string, 0, len(body.Entries))
	for name := range body.Entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		v := s.exec(r, 0, "CONFIG", "SET", name, body.Entries[name])
		if v.Kind == resp.KindError {
			writeError(w, http.StatusBadRequest, v.Str)
			return
		}
	}
	statusOK(w)
}

// Flushdb implements POST /api/v1/flushdb: FLUSHALL when all is set, else
// FLUSHDB against the requested logical DB (default 0).
func (s *Server) Flushdb(w http.ResponseWriter, r *http.Request) {
	var body httpapigen.FlushRequest
	if !JsonBody(w, r, &body) {
		return
	}
	db := 0
	if body.Db != nil {
		db = *body.Db
	}
	var v resp.Value
	if body.All != nil && *body.All {
		v = s.exec(r, db, "FLUSHALL")
	} else {
		v = s.exec(r, db, "FLUSHDB")
	}
	if v.Kind == resp.KindError {
		writeError(w, http.StatusBadRequest, v.Str)
		return
	}
	statusOK(w)
}

// persist503 is the save-family reply when no persistence manager is
// configured (bare/test servers).
func (s *Server) persistUnavailable(w http.ResponseWriter) {
	writeError(w, http.StatusServiceUnavailable, "persistence not configured")
}

// Save implements POST /api/v1/save (synchronous snapshot).
func (s *Server) Save(w http.ResponseWriter, _ *http.Request) {
	if s.persist == nil {
		s.persistUnavailable(w)
		return
	}
	if err := s.persist.Save(); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	statusOK(w)
}

// Bgsave implements POST /api/v1/bgsave.
func (s *Server) Bgsave(w http.ResponseWriter, _ *http.Request) {
	if s.persist == nil {
		s.persistUnavailable(w)
		return
	}
	if err := s.persist.BGSave(); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	statusOK(w)
}

// Bgrewriteaof implements POST /api/v1/bgrewriteaof.
func (s *Server) Bgrewriteaof(w http.ResponseWriter, _ *http.Request) {
	if s.persist == nil {
		s.persistUnavailable(w)
		return
	}
	if err := s.persist.BGRewriteAOF(); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	statusOK(w)
}

// interface guard: Server must satisfy the generated contract bindings.
var _ httpapigen.ServerInterface = (*Server)(nil)
