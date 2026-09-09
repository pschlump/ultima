package httpapi

// Prometheus metrics for the management surface (§10.1, GET /metrics):
// chiprom-style HTTP middleware (request counter + duration histogram,
// labeled by chi route pattern) plus a scrape-time collector over the
// engine's M6c introspection counters. Everything lives on a dedicated
// registry — the global prometheus registry stays untouched. /metrics is
// guarded by the server.metrics_allow IP allowlist, not JWT.

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pschlump/ultima/lib/commands"
)

// promRecorder captures the response status for the metrics labels. Like
// lib/handler's statusRecorder it forwards Unwrap/Hijack so middleware
// stacks that reach the underlying ResponseWriter (http.ResponseController,
// the WS upgrader) keep working through it.
type promRecorder struct {
	http.ResponseWriter
	status int
}

func (pr *promRecorder) WriteHeader(code int) {
	pr.status = code
	pr.ResponseWriter.WriteHeader(code)
}

// Unwrap lets middleware chains reach the underlying ResponseWriter.
func (pr *promRecorder) Unwrap() http.ResponseWriter { return pr.ResponseWriter }

// Hijack forwards to the underlying ResponseWriter (gorilla/websocket
// type-asserts http.Hijacker during the upgrade).
func (pr *promRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := pr.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("httpapi: underlying ResponseWriter is not a http.Hijacker")
	}
	return h.Hijack()
}

// PromMiddleware records ultima_http_requests_total{method,path,code} and
// ultima_http_request_duration_seconds{method,path}. The path label is the
// chi route pattern (RoutePath is only populated after routing, so it is
// read once next returns), keeping cardinality bounded. /metrics itself
// and /ws/v1 are skipped.
func (s *Server) PromMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || r.URL.Path == "/ws/v1" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		pr := &promRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(pr, r)
		path := chi.RouteContext(r.Context()).RoutePath
		if path == "" {
			path = "unmatched"
		}
		s.reqs.WithLabelValues(r.Method, path, strconv.Itoa(pr.status)).Inc()
		s.durs.WithLabelValues(r.Method, path).Observe(time.Since(start).Seconds())
	})
}

// engineCollector exports engine gauges/counters, read fresh at scrape
// time (prometheus.Collector contract).
type engineCollector struct {
	eng *commands.Engine
}

var (
	uptimeDesc      = prometheus.NewDesc("ultima_uptime_seconds", "Server uptime.", nil, nil)
	connClientsDesc = prometheus.NewDesc("ultima_connected_clients", "Live client connections.", nil, nil)
	totalConnsDesc  = prometheus.NewDesc("ultima_connections_total", "Connections received since start.", nil, nil)
	totalCmdsDesc   = prometheus.NewDesc("ultima_commands_processed_total", "Commands executed since start.", nil, nil)
	blockedDesc     = prometheus.NewDesc("ultima_blocked_clients", "Clients parked in a blocking command.", nil, nil)
	keysDesc        = prometheus.NewDesc("ultima_keyspace_keys", "Keys per logical DB.", []string{"db"}, nil)
	expiresDesc     = prometheus.NewDesc("ultima_keyspace_expires", "Keys with a TTL per logical DB.", []string{"db"}, nil)
	usedMemDesc     = prometheus.NewDesc("ultima_memory_used_bytes", "Keyspace memory estimate (INFO used_memory).", nil, nil)
	expiredDesc     = prometheus.NewDesc("ultima_expired_keys_total", "Keys removed by expiry.", nil, nil)
	evictedDesc     = prometheus.NewDesc("ultima_evicted_keys_total", "Keys removed by maxmemory eviction.", nil, nil)
	slowlogLenDesc  = prometheus.NewDesc("ultima_slowlog_len", "Slowlog ring occupancy.", nil, nil)
)

func (c *engineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- uptimeDesc
	ch <- connClientsDesc
	ch <- totalConnsDesc
	ch <- totalCmdsDesc
	ch <- blockedDesc
	ch <- keysDesc
	ch <- expiresDesc
	ch <- usedMemDesc
	ch <- expiredDesc
	ch <- evictedDesc
	ch <- slowlogLenDesc
}

func (c *engineCollector) Collect(ch chan<- prometheus.Metric) {
	e := c.eng
	ch <- prometheus.MustNewConstMetric(uptimeDesc, prometheus.GaugeValue, time.Since(e.Started).Seconds())
	ch <- prometheus.MustNewConstMetric(connClientsDesc, prometheus.GaugeValue, float64(e.ConnectedClients()))
	ch <- prometheus.MustNewConstMetric(totalConnsDesc, prometheus.CounterValue, float64(e.TotalConnections()))
	ch <- prometheus.MustNewConstMetric(totalCmdsDesc, prometheus.CounterValue, float64(e.TotalCommands()))
	ch <- prometheus.MustNewConstMetric(blockedDesc, prometheus.GaugeValue, float64(e.BlockedClients()))
	for _, st := range e.Shards.DBStats() {
		db := strconv.Itoa(st[0])
		ch <- prometheus.MustNewConstMetric(keysDesc, prometheus.GaugeValue, float64(st[1]), db)
		ch <- prometheus.MustNewConstMetric(expiresDesc, prometheus.GaugeValue, float64(st[2]), db)
	}
	ch <- prometheus.MustNewConstMetric(usedMemDesc, prometheus.GaugeValue, float64(e.Shards.UsedBytes()))
	ch <- prometheus.MustNewConstMetric(expiredDesc, prometheus.CounterValue, float64(e.Shards.ExpiredKeys.Load()))
	ch <- prometheus.MustNewConstMetric(evictedDesc, prometheus.CounterValue, float64(e.Shards.EvictedKeys.Load()))
	ch <- prometheus.MustNewConstMetric(slowlogLenDesc, prometheus.GaugeValue, float64(e.SlowlogLen()))
}

// GetMetrics implements GET /metrics: the caller's IP must match the
// metrics allowlist (empty list → loopback only); then the dedicated
// registry is served in the Prometheus text exposition format.
func (s *Server) GetMetrics(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	if ip == nil || !s.metricsAllowed(ip) {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}

// metricsAllowed reports whether ip matches the allowlist; an empty list
// means loopback only (the config default 127.0.0.0/8,::1 is resolved in
// cmd/ultima-server, this is the bare-server fallback).
func (s *Server) metricsAllowed(ip net.IP) bool {
	if len(s.metricsAllow) == 0 {
		return ip.IsLoopback()
	}
	for _, n := range s.metricsAllow {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}
