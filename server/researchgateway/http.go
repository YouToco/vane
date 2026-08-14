package researchgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const maxRequestBytesV1 = 1024
const effectTimeoutV1 = 3 * time.Minute
const clientRetryDelayV1 = 5 * time.Second

func (s *ServiceV1) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBytesV1+1))
	decoder.DisallowUnknownFields()
	var request ExecuteRequestV1
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	// Once a validated request reaches the claim boundary, client disconnects
	// must not cancel a possibly-paid provider effect or its settlement.
	effectCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), effectTimeoutV1)
	defer cancel()
	response, err := s.Execute(effectCtx, request)
	if err != nil {
		http.Error(w, "gateway execution failed", http.StatusUnprocessableEntity)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if response.Status == StatusInFlightV1 {
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusAccepted)
	}
	_ = json.NewEncoder(w).Encode(response)
}

// NewUnixClientV1 is the production constructor. No TCP fallback is provided;
// tests may inject an HTTP client/listener explicitly through NewClientV1.
func NewUnixClientV1(socketPath string) (*ClientV1, error) {
	if strings.TrimSpace(socketPath) == "" {
		return nil, errors.New("research gateway socket path is required")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}}
	return NewClientV1(&http.Client{Transport: transport}, "http://research-gateway")
}

type ClientV1 struct {
	httpClient *http.Client
	baseURL    string
	retryDelay time.Duration
}

func NewClientV1(httpClient *http.Client, baseURL string) (*ClientV1, error) {
	if httpClient == nil || baseURL == "" {
		return nil, errors.New("research gateway client requires transport and URL")
	}
	return &ClientV1{httpClient: httpClient, baseURL: baseURL,
		retryDelay: clientRetryDelayV1}, nil
}

func (c *ClientV1) Execute(ctx context.Context, binding ExecuteRequestV1) (ExecuteResponseV1, error) {
	if err := binding.Validate(); err != nil {
		return ExecuteResponseV1{}, err
	}
	payload, err := json.Marshal(binding)
	if err != nil {
		return ExecuteResponseV1{}, err
	}
	for {
		result, retry, err := c.executeOnce(ctx, payload)
		if err == nil && !retry {
			return result, nil
		}
		if err != nil && !retry {
			return ExecuteResponseV1{}, err
		}
		timer := time.NewTimer(c.retryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ExecuteResponseV1{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *ClientV1) executeOnce(ctx context.Context, payload []byte) (
	ExecuteResponseV1, bool, error,
) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+ExecutePathV1, bytes.NewReader(payload))
	if err != nil {
		return ExecuteResponseV1{}, false, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		// The request may have crossed the paid-effect claim boundary. Retry the
		// same immutable binding until the Activity deadline; the gateway's
		// first-writer ledger prevents another Provider call.
		return ExecuteResponseV1{}, ctx.Err() == nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return ExecuteResponseV1{}, false, errors.New("research gateway rejected execution")
	}
	var result ExecuteResponseV1
	decoder := json.NewDecoder(io.LimitReader(response.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return ExecuteResponseV1{}, true, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return ExecuteResponseV1{}, true,
			errors.New("research gateway response has trailing data")
	}
	if result.Status != StatusSettledV1 && result.Status != StatusInFlightV1 {
		return ExecuteResponseV1{}, true,
			errors.New("research gateway returned invalid status")
	}
	if (response.StatusCode == http.StatusOK) != (result.Status == StatusSettledV1) {
		return ExecuteResponseV1{}, true,
			errors.New("research gateway HTTP status differs from body")
	}
	return result, result.Status == StatusInFlightV1, nil
}
