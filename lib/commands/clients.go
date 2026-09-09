package commands

// Client introspection and kill for the M6c management API (design doc
// §10.1: GET /api/v1/clients, POST /api/v1/clients/{id}/kill). The engine
// keeps a registry of live connections; front-ends set a kill hook and a
// surface label on their ConnState so the API can list and close
// connections uniformly across RESP, gRPC and WS.

import "time"

// ClientInfo is a point-in-time snapshot of one live connection, the
// HTTP-API analogue of CLIENT LIST. All fields are copied under the
// connection's metaMu, so reads never race the connection goroutine.
type ClientInfo struct {
	ID            uint64
	Addr          string
	Name          string
	User          string // JWT identity on the gated surfaces (M6a); "" on RESP
	DB            int
	Proto         int // negotiated RESP version; 0 on binary surfaces
	Surface       string
	AgeSeconds    int64
	IdleSeconds   int64
	LastCommand   string
	Subscriptions int
	Patterns      int
}

// noteCommand records per-connection introspection metadata after each
// executed command: last command name, activity timestamp, and a copy of
// the fields (db, name, user, subscription counts) the client registry
// serves cross-goroutine. Runs on the connection goroutine at the end of
// Execute, so it sees post-command state (SELECT, CLIENT SETNAME, AUTH).
func (cs *ConnState) noteCommand(name string) {
	cs.lastActivityUnix.Store(time.Now().Unix())
	cs.metaMu.Lock()
	cs.lastCmd = name
	cs.metaDB = cs.DB
	cs.metaName = cs.Name
	cs.metaUser = cs.User
	cs.metaSubs = len(cs.subs) + len(cs.psubs)
	cs.metaPsubs = len(cs.psubs)
	cs.metaMu.Unlock()
}

// SetSurface labels the connection's front-end ("resp", "grpc", "ws")
// for client introspection. Called once right after NewConnState.
func (cs *ConnState) SetSurface(surface string) {
	cs.metaMu.Lock()
	cs.surface = surface
	cs.metaMu.Unlock()
}

// SetKillFunc installs the front-end hook KillClient invokes to close
// this connection (RESP socket close, WS close). Front-ends that cannot
// be force-closed leave it nil, making the connection unkillable.
func (cs *ConnState) SetKillFunc(fn func()) {
	cs.metaMu.Lock()
	cs.onKill = fn
	cs.metaMu.Unlock()
}

// ListClients snapshots every live connection (CLIENT LIST for the HTTP
// API, §10.1). Synthetic internal connections (AOF replay, HTTP key
// preview) are never registered.
func (e *Engine) ListClients() []ClientInfo {
	now := time.Now().Unix()
	e.clientsMu.Lock()
	conns := make([]*ConnState, 0, len(e.clients))
	for _, cs := range e.clients {
		conns = append(conns, cs)
	}
	e.clientsMu.Unlock()
	out := make([]ClientInfo, 0, len(conns))
	for _, cs := range conns {
		cs.metaMu.Lock()
		idle := now - cs.lastActivityUnix.Load()
		if idle < 0 {
			idle = 0
		}
		out = append(out, ClientInfo{
			ID:            cs.ID,
			Addr:          cs.Addr,
			Name:          cs.metaName,
			User:          cs.metaUser,
			DB:            cs.metaDB,
			Proto:         cs.Proto,
			Surface:       cs.surface,
			AgeSeconds:    now - cs.Created.Unix(),
			IdleSeconds:   idle,
			LastCommand:   cs.lastCmd,
			Subscriptions: cs.metaSubs,
			Patterns:      cs.metaPsubs,
		})
		cs.metaMu.Unlock()
	}
	return out
}

// KillClient closes the connection with the given id through its
// front-end kill hook. found reports whether the id is live; killable
// reports whether the front-end installed a hook (binary streams that
// cannot be force-closed are listed but not killable).
func (e *Engine) KillClient(id uint64) (found, killable bool) {
	e.clientsMu.Lock()
	cs, ok := e.clients[id]
	e.clientsMu.Unlock()
	if !ok {
		return false, false
	}
	cs.metaMu.Lock()
	fn := cs.onKill
	cs.metaMu.Unlock()
	if fn == nil {
		return true, false
	}
	fn()
	return true, true
}

// --- counters for /metrics (§10.1) --------------------------------------

// ConnectedClients is INFO's connected_clients.
func (e *Engine) ConnectedClients() int64 { return e.conns.Load() }

// TotalConnections is INFO's total_connections_received.
func (e *Engine) TotalConnections() int64 { return e.totalConns.Load() }

// TotalCommands is INFO's total_commands_processed.
func (e *Engine) TotalCommands() int64 { return e.totalCmds.Load() }

// BlockedClients is INFO's blocked_clients.
func (e *Engine) BlockedClients() int64 { return e.blockedClients.Load() }
