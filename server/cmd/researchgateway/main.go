package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/YouToco/vane/llm"
	"github.com/YouToco/vane/researchgateway"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "vane research gateway: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := researchgateway.LoadProcessConfigV1()
	if err != nil {
		return err
	}
	repository, err := researchgateway.NewPostgresRepositoryV1(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer repository.Close()
	routes := make([]llm.RuntimeModelRouteV1, 0, len(cfg.Routes))
	for _, route := range cfg.Routes {
		routes = append(routes, llm.RuntimeModelRouteV1{
			Provider: route.Provider, Endpoint: route.Endpoint,
			CredentialRef: route.CredentialRef, Client: llm.New(route.LLM),
		})
	}
	resolver, err := llm.NewRuntimeModelResolverV1(routes...)
	if err != nil {
		return err
	}
	service, err := researchgateway.NewServiceV1(repository,
		researchgateway.LLMProviderV1{Resolver: resolver})
	if err != nil {
		return err
	}
	listener, err := activatedListener()
	if err != nil {
		return err
	}
	defer listener.Close()
	listener, err = researchgateway.WrapPeerUIDListenerV1(listener, cfg.AllowedUID)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: service.Handler(), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	select {
	case err := <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := service.Wait(shutdownCtx); err != nil {
		return err
	}
	return nil
}

func activatedListener() (net.Listener, error) {
	pid, _ := strconv.Atoi(os.Getenv("LISTEN_PID"))
	fds, _ := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if pid != os.Getpid() || fds != 1 {
		return nil, fmt.Errorf("research gateway requires exactly one systemd socket activation fd")
	}
	file := os.NewFile(uintptr(3), "research-gateway.socket")
	if file == nil {
		return nil, fmt.Errorf("research gateway activation fd is unavailable")
	}
	defer file.Close()
	listener, err := net.FileListener(file)
	if err != nil {
		return nil, err
	}
	if listener.Addr().Network() != "unix" {
		listener.Close()
		return nil, fmt.Errorf("research gateway activation listener is not unix")
	}
	return listener, nil
}
