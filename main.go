// Command ntfy-mcp is a standalone Streamable-HTTP MCP gateway for ntfy. It
// exposes two tools and the SEP-3415 events extension with push delivery over
// server-sent events.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/naqerl/ntfy-mcp/internal/config"
	"github.com/naqerl/ntfy-mcp/internal/events"
	"github.com/naqerl/ntfy-mcp/internal/mcpx"
	"github.com/naqerl/ntfy-mcp/internal/ntfy"
	"github.com/naqerl/ntfy-mcp/internal/tools"
)

// VERSION is set at build time with
// -ldflags "-X main.VERSION=v0.1.0".
var VERSION = "dev"

func main() {
	var (
		showVersion bool
		addrFlag    string
		pathFlag    string
	)
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.StringVar(&addrFlag, "addr", "", "override MCP_ADDR (e.g. :8080)")
	flag.StringVar(&pathFlag, "path", "", "override MCP_PATH (e.g. /mcp)")
	flag.Parse()

	if showVersion {
		fmt.Println(VERSION)
		return
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "ntfy-mcp:", err)
		os.Exit(1)
	}
	if addrFlag != "" {
		cfg.Addr = addrFlag
	}
	if pathFlag != "" {
		cfg.Path = pathFlag
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "ntfy-mcp:", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	upstream := ntfy.New(cfg.NtfyBaseURL, cfg.NtfyToken, &http.Client{})
	toolSet := tools.New(upstream, cfg.DefaultTopic)
	eventSet := events.New(upstream, events.Options{
		Heartbeat: cfg.Heartbeat,
		PollMS:    cfg.PollMS,
		MaxEvents: cfg.MaxEvents,
		StreamMax: cfg.StreamMax,
	})
	mcpServer := mcpx.NewServer(VERSION, toolSet, eventSet)
	mcpHandler := mcpx.NewHandler(mcpServer, cfg.AuthToken)

	mux := http.NewServeMux()
	mux.Handle(cfg.Path, mcpHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		fmt.Fprintf(w, `{"status":"ok","version":%q}`+"\n", VERSION)
	})

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// WriteTimeout must stay 0: events/stream responses are long-lived
		// text/event-stream bodies.
		WriteTimeout: 0,
		IdleTimeout:  2 * time.Minute,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown", "error", err)
		}
	}()

	logger.Info("ntfy-mcp starting",
		"version", VERSION,
		"addr", cfg.Addr,
		"path", cfg.Path,
		"ntfy_base_url", cfg.NtfyBaseURL.String(),
	)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("listen", "error", err)
		os.Exit(1)
	}
	logger.Info("ntfy-mcp stopped")
}
