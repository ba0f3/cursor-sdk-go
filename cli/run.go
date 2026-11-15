// Package cli runs the standalone native Cursor SDK bridge.
package cli

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ba0f3/cursor-sdk-go/bridge"
)

/**
 * @brief Run the standalone bridge until signal cancellation or an authenticated Shutdown RPC.
 */
func Run() {
	opts := cursorbridgeruntime.OptionsFromEnv()
	host := flag.String("host", opts.Host, "Host to bind")
	port := flag.Int("port", opts.Port, "Port to bind (0 = ephemeral)")
	workspace := flag.String("workspace", opts.Workspace, "Workspace root")
	stateRoot := flag.String("state-root", opts.StateRoot, "Bridge state root")
	flag.Func("local-store", "Bridge-owned store configuration", func(value string) error {
		opts.LocalStoreJSON = value
		return nil
	})
	flag.Func("tool-callback-url", "SDK custom tool callback URL", func(value string) error {
		opts.ToolCallbackURL = value
		return nil
	})
	flag.Func("tool-callback-auth-token", "SDK custom tool callback authentication token", func(value string) error {
		opts.ToolCallbackToken = value
		return nil
	})
	flag.Func("store-callback-url", "External store callback URL; unavailable in this runtime", func(value string) error {
		opts.StoreCallbackURL = value
		return nil
	})
	flag.Func("store-callback-auth-token", "External store callback authentication token; unavailable in this runtime", func(value string) error {
		opts.StoreCallbackToken = value
		return nil
	})
	verbose := flag.Bool("verbose", opts.Verbose, "Log RPC diagnostics to stderr")
	flag.Parse()
	opts.Host = *host
	opts.Port = *port
	opts.Workspace = *workspace
	opts.StateRoot = *stateRoot
	opts.Verbose = *verbose
	opts.Stderr = os.Stderr

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server, err := cursorbridgeruntime.Start(ctx, opts)
	if err != nil {
		log.Fatalf("start native cursor-sdk-bridge: %v", err)
	}
	select {
	case <-ctx.Done():
	case <-server.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Close(shutdownCtx)
}
