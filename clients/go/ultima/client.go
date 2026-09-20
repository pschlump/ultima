// Package ultima umbrella client (design doc §11.1): one Client tying the
// three command
// surfaces (RESP §6.1, gRPC §6.2, WS §6.3) and the REST management API
// (§10.1) together, sharing the §9.3 token manager across the JWT-gated
// surfaces. Surfaces connect lazily on first access.
package ultima

import (
	"context"
	"sync"

	"github.com/pschlump/ultima/lib/resp"
)

// Value is the universal reply type of the client library — the engine's
// own resp.Value (decision D3), re-exported so callers need one import.
type Value = resp.Value

// Options configures New. Empty surface addresses disable that surface.
type Options struct {
	RespAddr string // host:port of the RESP surface (:6379)
	GrpcAddr string // host:port of the gRPC surface (:6380)
	HTTPAddr string // host:port of the HTTP/WS/REST surface (:6381)

	// Management auth (§9.3): Username/Password/TOTP drive a TokenManager
	// against HTTPAddr; Token is a static access token instead.
	Username string
	Password string
	TOTP     string
	Token    string

	// RESP-only knobs (redis-cli analogues).
	PasswordRESP string // requirepass AUTH on connect (-a)
	DB           int    // SELECT on connect; < 0 skips (default 0 selects DB 0)
	RESP3        bool   // negotiate RESP3 via HELLO

	// WS knobs (§9.4).
	DisableSessions bool
	OnPush          func(v Value, pushSeq uint64)
	OnGap           func()
	OnResume        func()
}

// Client is the umbrella client. Goroutine-safe; Close releases every
// connected surface.
type Client struct {
	opts Options

	tm *TokenManager // nil when no management credentials are configured

	mu   sync.Mutex
	resp *RESPClient
	grpc *GRPCClient
	ws   *WSClient
	rest *RESTClient
}

// New builds the umbrella client; nothing connects until the surface
// accessors are called.
func New(opts Options) *Client {
	c := &Client{opts: opts}
	if opts.Token == "" && opts.Username != "" && opts.HTTPAddr != "" {
		c.tm = NewTokenManager(opts.HTTPAddr, Credentials{
			Username: opts.Username, Password: opts.Password, TOTP: opts.TOTP,
		})
	}
	return c
}

// tokenFunc resolves the token provider shared by the JWT-gated surfaces:
// the static token when set, else the TokenManager.
func (c *Client) tokenFunc() func(context.Context) (string, error) {
	if c.opts.Token != "" {
		return func(context.Context) (string, error) { return c.opts.Token, nil }
	}
	if c.tm != nil {
		return c.tm.Token
	}
	return nil
}

// REST returns the management-API client (§10.1). Requires HTTPAddr.
func (c *Client) REST() *RESTClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rest == nil {
		var opts []RESTOption
		if f := c.tokenFunc(); f != nil {
			opts = append(opts, WithRESTTokenFunc(f))
		}
		c.rest = NewRESTClient(c.opts.HTTPAddr, opts...)
	}
	return c.rest
}

// RESP connects (once) to the RESP surface (§6.1).
func (c *Client) RESP() (*RESPClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.resp != nil {
		return c.resp, nil
	}
	opts := []RESPOption{WithRESPPassword(c.opts.PasswordRESP), WithRESPDB(c.opts.DB)}
	if c.opts.RESP3 {
		opts = append(opts, WithRESPProto(3))
	}
	r, err := DialRESP(c.opts.RespAddr, opts...)
	if err != nil {
		return nil, err
	}
	c.resp = r
	return r, nil
}

// GRPC connects (once) to the gRPC surface (§6.2).
func (c *Client) GRPC() (*GRPCClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.grpc != nil {
		return c.grpc, nil
	}
	var opts []GRPCOption
	if f := c.tokenFunc(); f != nil {
		opts = append(opts, WithGRPCTokenFunc(f))
	}
	g, err := DialGRPC(c.opts.GrpcAddr, opts...)
	if err != nil {
		return nil, err
	}
	c.grpc = g
	return g, nil
}

// WS connects (once) to the WebSocket surface (§6.3) with §9.4 recovery.
func (c *Client) WS() (*WSClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ws != nil {
		return c.ws, nil
	}
	opts := WSOptions{
		Addr:            c.opts.HTTPAddr,
		DisableSessions: c.opts.DisableSessions,
		OnPush:          c.opts.OnPush,
		OnGap:           c.opts.OnGap,
		OnResume:        c.opts.OnResume,
	}
	if f := c.tokenFunc(); f != nil {
		opts.TokenProvider = f
	}
	w, err := DialWS(opts)
	if err != nil {
		return nil, err
	}
	c.ws = w
	return w, nil
}

// Close releases every connected surface.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.resp != nil {
		_ = c.resp.Close()
	}
	if c.grpc != nil {
		_ = c.grpc.Close()
	}
	if c.ws != nil {
		_ = c.ws.Close()
	}
	return nil
}

// --- typed result helpers on Value -------------------------------------------

// IsError reports whether v is an error reply (KindError) — replies the
// server rejected, as opposed to transport failures (which surface as Go
// errors from the Exec family).
func IsError(v Value) bool { return v.Kind == resp.KindError }

// AsString extracts v as a string: simple strings, blob strings,
// verbatim payloads, big numbers, and errors all convert.
func AsString(v Value) (string, bool) {
	switch v.Kind {
	case resp.KindSimpleString, resp.KindError, resp.KindBigNumber:
		return v.Str, true
	case resp.KindBlobString:
		if v.Blob == nil {
			return "", false // RESP2 null bulk
		}
		return string(v.Blob), true
	case resp.KindVerbatim:
		return string(v.Blob), true
	}
	return "", false
}

// AsInt extracts an integer reply.
func AsInt(v Value) (int64, bool) {
	if v.Kind == resp.KindInt {
		return v.Int, true
	}
	return 0, false
}

// AsFloat extracts a double reply (or an integer as float).
func AsFloat(v Value) (float64, bool) {
	switch v.Kind {
	case resp.KindDouble:
		return v.Dbl, true
	case resp.KindInt:
		return float64(v.Int), true
	}
	return 0, false
}

// AsBool extracts a boolean reply.
func AsBool(v Value) (bool, bool) {
	if v.Kind == resp.KindBool {
		return v.Bool, true
	}
	return false, false
}

// IsNull reports whether v is the null reply (missing key, …).
func IsNull(v Value) bool {
	return v.Kind == resp.KindNull || (v.Kind == resp.KindBlobString && v.Blob == nil)
}

// AsStringSlice extracts an array/set/push reply as strings; elements
// that do not convert yield ok = false.
func AsStringSlice(v Value) ([]string, bool) {
	if v.Kind != resp.KindArray && v.Kind != resp.KindSet && v.Kind != resp.KindPush {
		return nil, false
	}
	out := make([]string, 0, len(v.Arr))
	for _, e := range v.Arr {
		s, ok := AsString(e)
		if !ok {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}

// AsStringMap extracts a map reply (KindMap, flattened k,v pairs) as a
// Go map.
func AsStringMap(v Value) (map[string]string, bool) {
	if v.Kind != resp.KindMap {
		return nil, false
	}
	out := make(map[string]string, len(v.Arr)/2)
	for i := 0; i+1 < len(v.Arr); i += 2 {
		k, ok1 := AsString(v.Arr[i])
		val, ok2 := AsString(v.Arr[i+1])
		if !ok1 || !ok2 {
			return nil, false
		}
		out[k] = val
	}
	return out, true
}
