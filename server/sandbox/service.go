package sandbox

import (
	"context"
	"errors"
	"time"
)

var (
	ErrPolicy      = errors.New("sandbox policy is not approved")
	ErrOutputLimit = errors.New("sandbox output exceeded approved limit")
)

// Backend is implemented by the isolated sandboxd Firecracker launcher, never
// by the Vane server process.
type Backend interface {
	Run(context.Context, Request) (Result, error)
}

type ServiceConfig struct {
	MaxInputBytes        int
	AllowedPolicyDigests map[string]struct{}
	Now                  func() time.Time
}

type Service struct {
	config ServiceConfig
}

func NewService(config ServiceConfig) (*Service, error) {
	if config.MaxInputBytes < 1 || len(config.AllowedPolicyDigests) == 0 {
		return nil, errors.New("sandbox service configuration is incomplete")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	for digest := range config.AllowedPolicyDigests {
		if err := requireSHA256("allowed policy", digest); err != nil {
			return nil, err
		}
	}
	return &Service{config: config}, nil
}

// Execute validates the complete authority envelope and then remains dark.
// There is deliberately no injected backend and no in-memory replay map: until
// sandboxd has a durable append-only invocation ledger and a reviewed guest I/O
// protocol, no production call can reach Firecracker or pretend to be durable.
func (s *Service) Execute(ctx context.Context, request Request) (Result, error) {
	if err := request.Validate(s.config.MaxInputBytes); err != nil {
		return Result{}, err
	}
	if _, ok := s.config.AllowedPolicyDigests[request.PolicyDigest]; !ok {
		return Result{}, ErrPolicy
	}
	started := s.config.Now()
	select {
	case <-ctx.Done():
		return Result{InvocationID: request.InvocationID, Status: "disabled",
			Duration: s.config.Now().Sub(started), ErrorCode: "dark_foundation"}, ctx.Err()
	default:
	}
	return Result{InvocationID: request.InvocationID, Status: "disabled",
		Duration: s.config.Now().Sub(started), ErrorCode: "dark_foundation"}, ErrDarkFoundation
}
