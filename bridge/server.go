package cursorbridgeruntime

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
	"github.com/ba0f3/cursor-sdk-go/bridge/localrun"
)

/**
 * @brief A loopback sdk.v1 Connect server started by the native bridge runtime.
 */
type Server struct {
	opts       Options
	token      string
	tokenFile  string
	listener   net.Listener
	httpServer *http.Server
	engine     *localrun.Engine

	mu     sync.Mutex
	closed bool
	done   chan struct{} ///< Closed after the HTTP server and active runs finish shutdown.
}

/**
 * @brief Start a native bridge HTTP server and return its discovery metadata.
 */
func Start(ctx context.Context, opts Options) (*Server, error) {
	if err := validateStoreOptions(opts); err != nil {
		return nil, err
	}
	if opts.Host == "" {
		opts.Host = "127.0.0.1"
	}
	if strings.TrimSpace(opts.Workspace) == "" {
		dir, err := os.MkdirTemp("", "ccload-native-bridge-")
		if err != nil {
			return nil, err
		}
		opts.Workspace = dir
	}
	if strings.TrimSpace(opts.StateRoot) == "" {
		opts.StateRoot = filepath.Join(opts.Workspace, "state")
		if err := os.MkdirAll(opts.StateRoot, 0o700); err != nil {
			return nil, err
		}
	}
	engine, err := localrun.NewPersistentEngine(cursorapi.NewClient(nil), opts.StateRoot)
	if err != nil {
		return nil, err
	}
	engine.SetToolCallback(opts.ToolCallbackURL, opts.ToolCallbackToken)
	token, tokenPath, err := writeAuthTokenFile(opts.Workspace)
	if err != nil {
		engine.Close()
		return nil, err
	}
	services := newBridgeServices(engine)
	mux := http.NewServeMux()
	agentPath, agentHandler := newConnectHandler(services, "SdkAgentService")
	cursorPath, cursorHandler := newConnectHandler(services, "SdkCursorService")
	controlPath, controlHandler := newConnectHandler(services, "SdkBridgeControlService")
	mux.Handle(agentPath, agentHandler)
	mux.Handle(cursorPath, cursorHandler)
	mux.Handle(controlPath, controlHandler)

	ln, err := net.Listen("tcp", net.JoinHostPort(opts.Host, fmt.Sprint(opts.Port)))
	if err != nil {
		engine.Close()
		return nil, err
	}
	if tcp, ok := ln.Addr().(*net.TCPAddr); ok && opts.Port == 0 {
		opts.Port = tcp.Port
	}
	srv := &http.Server{Handler: requireBearer(mux, token)}
	server := &Server{
		opts: opts, token: token, tokenFile: tokenPath,
		listener: ln, httpServer: srv, engine: engine,
		done: make(chan struct{}),
	}
	services.shutdown = func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Close(shutdownCtx)
	}
	go func() {
		_ = srv.Serve(ln)
	}()
	if err := writeReadyLine(opts.Stderr, opts.Host, opts.Port, opts.Workspace, opts.StateRoot, tokenPath); err != nil {
		_ = server.Close(ctx)
		return nil, err
	}
	return server, nil
}

func newConnectHandler(services *bridgeServices, service string) (string, http.Handler) {
	switch service {
	case "SdkAgentService":
		pattern, handler := sdkAgentHandler(services)
		return pattern, handler
	case "SdkCursorService":
		pattern, handler := sdkCursorHandler(services)
		return pattern, handler
	default:
		pattern, handler := sdkControlHandler(services)
		return pattern, handler
	}
}

// BaseURL returns the loopback origin clients should dial.
func (s *Server) BaseURL() string {
	if s == nil || s.listener == nil {
		return ""
	}
	host := s.opts.Host
	port := s.opts.Port
	return fmt.Sprintf("http://%s", net.JoinHostPort(host, fmt.Sprint(port)))
}

// AuthToken returns the bridge bearer token for RPC authorization headers.
func (s *Server) AuthToken() string {
	if s == nil {
		return ""
	}
	return s.token
}

// TokenFile returns the on-disk bearer token path advertised in discovery.
func (s *Server) TokenFile() string {
	if s == nil {
		return ""
	}
	return s.tokenFile
}

// Workspace returns the bridge workspace directory.
func (s *Server) Workspace() string {
	if s == nil {
		return ""
	}
	return s.opts.Workspace
}

/**
 * @brief Cancel active runs and callbacks before stopping the bridge HTTP server.
 * @param ctx Shutdown deadline; active connections are forcibly closed if it expires.
 * @return Always nil, including repeated shutdown calls.
 * @note Concurrent-safe; engine cancellation occurs before waiting for HTTP handlers.
 */
func (s *Server) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	engineDone := make(chan struct{})
	go func() {
		if s.engine != nil {
			s.engine.Close()
		}
		close(engineDone)
	}()
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			_ = s.httpServer.Close()
		}
	}
	select {
	case <-engineDone:
		close(s.done)
	case <-ctx.Done():
		go func() { <-engineDone; close(s.done) }()
	}
	return nil
}

/**
 * @brief Observe completed bridge shutdown without terminating an embedding application.
 * @return Channel closed after shutdown, or nil for a nil server.
 */
func (s *Server) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}
