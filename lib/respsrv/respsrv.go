// Package respsrv is the M0 hand-rolled RESP2 listener: goroutine per
// connection, PING (inline and multibulk forms) answered with +PONG,
// everything else refused with -ERR. It is structured to be replaced by the
// vendored redcon fork (lib/resp, design doc §6.1) in M1.
package respsrv

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
)

// Server accepts RESP connections from a listener until Closed.
type Server struct {
	logger *slog.Logger

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// New returns a Server that logs through logger.
func New(logger *slog.Logger) *Server {
	return &Server{logger: logger, conns: make(map[net.Conn]struct{})}
}

// Serve accepts connections on lis until Close is called. It returns only
// listener-accept failures before Close.
func (s *Server) Serve(lis net.Listener) error {
	for {
		conn, err := lis.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("resp: accept: %w", err)
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return nil
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(conn)
	}
}

// Close drops every open connection; the listener itself is owned by the
// caller, which should close it to unblock Serve.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Server) handle(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()
	r := bufio.NewReader(conn)
	for {
		args, err := readCommand(r)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) {
				s.logger.Debug("resp: connection read error", "err", err)
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		if err := writeReply(conn, dispatch(args)); err != nil {
			return
		}
	}
}

// dispatch answers PING (with optional echo payload, matching Redis) and
// refuses everything else.
func dispatch(args []string) string {
	switch strings.ToUpper(args[0]) {
	case "PING":
		if len(args) > 1 {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
		}
		return "+PONG\r\n"
	default:
		return fmt.Sprintf("-ERR unknown command '%s'\r\n", args[0])
	}
}

func writeReply(conn net.Conn, reply string) error {
	_, err := io.WriteString(conn, reply)
	return err
}

// readCommand reads one command in either RESP multibulk form
// (*N $len arg ...) or inline/telnet form (whitespace-separated line).
func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if line == "" {
		return nil, nil
	}
	if line[0] != '*' {
		return strings.Fields(line), nil
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 {
		return nil, fmt.Errorf("resp: bad multibulk header %q", line)
	}
	args := make([]string, 0, n)
	for range n {
		hdr, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if hdr == "" || hdr[0] != '$' {
			return nil, fmt.Errorf("resp: expected bulk length, got %q", hdr)
		}
		size, err := strconv.Atoi(hdr[1:])
		if err != nil || size < 0 {
			return nil, fmt.Errorf("resp: bad bulk length %q", hdr)
		}
		buf := make([]byte, size+2) // payload + CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}
