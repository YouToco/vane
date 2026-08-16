package sandbox

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const (
	DefaultMaxConnections = 16
	MaxConnectionsLimit   = 64
	DefaultMaxWireBytes   = 256 << 10
	MaxWireBytesLimit     = 256 << 10
	MinWireBytesLimit     = 4 << 10
	MaxInputWireDivisor   = 4
	darkConnectionTimeout = 30 * time.Second
)

type wireResponse struct {
	Result    Result `json:"result"`
	ErrorCode string `json:"error_code,omitempty"`
}

type PeerAuthorizer interface {
	Authorize(*net.UnixConn) error
}

type UIDAuthorizer struct{ UID uint32 }

func (a UIDAuthorizer) Authorize(conn *net.UnixConn) error {
	uid, err := unixPeerUID(conn)
	if err != nil {
		return err
	}
	if uid != a.UID {
		return fmt.Errorf("sandbox socket peer uid %d is not approved", uid)
	}
	return nil
}

type Daemon struct {
	SocketPath               string
	Socket                   SocketContract
	Service                  *Service
	Authorizer               PeerAuthorizer
	MaxConnections           int
	MaxWireBytes             int
	connectionTimeoutForTest time.Duration
}

type SocketContract struct {
	ParentUID  uint32
	ParentGID  uint32
	ParentMode os.FileMode
	SocketUID  int
	SocketGID  int
	SocketMode os.FileMode
	// Tests in this package can exercise the wire protocol without root. This
	// field is unexported so no config file or external caller can weaken the
	// production root-owned socket contract.
	allowUnprivilegedForTest bool
}

func (d *Daemon) Serve(ctx context.Context) (returnErr error) {
	if d.Service == nil || d.Authorizer == nil || !filepath.IsAbs(d.SocketPath) {
		return errors.New("sandbox daemon configuration is incomplete")
	}
	if err := rejectUnsafeSocketPath(d.SocketPath, d.Socket); err != nil {
		return err
	}
	maxConnections, maxWireBytes, err := normalizeDaemonLimits(d.MaxConnections, d.MaxWireBytes)
	if err != nil {
		return err
	}
	d.MaxConnections, d.MaxWireBytes = maxConnections, maxWireBytes
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: d.SocketPath, Net: "unix"})
	if err != nil {
		return fmt.Errorf("listen sandbox socket: %w", err)
	}
	listener.SetUnlinkOnClose(false)
	socketInfo, err := os.Lstat(d.SocketPath)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("inspect created sandbox socket: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, cleanupSocket(listener, d.SocketPath, socketInfo))
	}()
	if err := os.Chown(d.SocketPath, d.Socket.SocketUID, d.Socket.SocketGID); err != nil {
		return fmt.Errorf("own sandbox socket: %w", err)
	}
	if err := os.Chmod(d.SocketPath, d.Socket.SocketMode); err != nil {
		return fmt.Errorf("protect sandbox socket: %w", err)
	}
	protectedInfo, err := os.Lstat(d.SocketPath)
	var uid, gid uint32
	var ownerOK bool
	if err == nil {
		uid, gid, ownerOK = fileOwner(protectedInfo)
	}
	if err != nil || !ownerOK || uid != uint32(d.Socket.SocketUID) ||
		gid != uint32(d.Socket.SocketGID) ||
		protectedInfo.Mode().Perm() != d.Socket.SocketMode.Perm() ||
		!os.SameFile(socketInfo, protectedInfo) {
		return errors.New("sandbox socket does not match exact leaf contract")
	}
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	connectionSlots := make(chan struct{}, d.MaxConnections)
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept sandbox socket: %w", err)
		}
		select {
		case connectionSlots <- struct{}{}:
			go func() {
				defer func() { <-connectionSlots }()
				d.handle(ctx, conn)
			}()
		default:
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			_ = writeWire(conn, wireResponse{ErrorCode: "busy"}, d.MaxWireBytes)
			_ = conn.Close()
		}
	}
}

func (d *Daemon) handle(ctx context.Context, conn *net.UnixConn) {
	defer conn.Close()
	timeout := darkConnectionTimeout
	if d.connectionTimeoutForTest > 0 {
		timeout = d.connectionTimeoutForTest
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if err := d.Authorizer.Authorize(conn); err != nil {
		_ = writeWire(conn, wireResponse{ErrorCode: "unauthorized_peer"}, d.MaxWireBytes)
		return
	}
	var request Request
	if err := readWire(conn, &request, d.MaxWireBytes); err != nil {
		_ = writeWire(conn, wireResponse{ErrorCode: "invalid_request"}, d.MaxWireBytes)
		return
	}
	result, err := d.Service.Execute(ctx, request)
	response := wireResponse{Result: result}
	if err != nil {
		response.ErrorCode = publicErrorCode(err)
	}
	_ = writeWire(conn, response, d.MaxWireBytes)
}

type Client struct {
	SocketPath   string
	MaxWireBytes int
}

func (c Client) Execute(ctx context.Context, request Request) (Result, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", c.SocketPath)
	if err != nil {
		return Result{}, fmt.Errorf("connect sandboxd: %w", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	maxWireBytes := c.MaxWireBytes
	if maxWireBytes == 0 {
		maxWireBytes = DefaultMaxWireBytes
	}
	if maxWireBytes < MinWireBytesLimit || maxWireBytes > MaxWireBytesLimit {
		return Result{}, errors.New("sandbox client wire limit is outside the closed range")
	}
	if err := writeWire(conn, request, maxWireBytes); err != nil {
		return Result{}, err
	}
	var response wireResponse
	if err := readWire(conn, &response, maxWireBytes); err != nil {
		return Result{}, err
	}
	if response.ErrorCode != "" {
		return response.Result, errors.New(response.ErrorCode)
	}
	return response.Result, nil
}

func readWire(reader io.Reader, target any, maxBytes int) error {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return fmt.Errorf("read sandbox frame length: %w", err)
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || uint64(length) > uint64(maxBytes) {
		return errors.New("sandbox frame length is outside the closed limit")
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return fmt.Errorf("read sandbox frame payload: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode sandbox message: %w", err)
	}
	if decoder.InputOffset() != int64(len(payload)) {
		return errors.New("sandbox message has trailing bytes")
	}
	return nil
}

func writeWire(writer io.Writer, value any, maxBytes int) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode sandbox message: %w", err)
	}
	if len(payload) == 0 || len(payload) > maxBytes {
		return errors.New("sandbox frame exceeds the closed wire limit")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeFull(writer, header[:]); err != nil {
		return fmt.Errorf("write sandbox frame length: %w", err)
	}
	if err := writeFull(writer, payload); err != nil {
		return fmt.Errorf("write sandbox frame payload: %w", err)
	}
	return nil
}

func normalizeDaemonLimits(maxConnections, maxWireBytes int) (int, int, error) {
	if maxConnections == 0 {
		maxConnections = DefaultMaxConnections
	}
	if maxWireBytes == 0 {
		maxWireBytes = DefaultMaxWireBytes
	}
	if maxConnections < 1 || maxConnections > MaxConnectionsLimit ||
		maxWireBytes < MinWireBytesLimit || maxWireBytes > MaxWireBytesLimit {
		return 0, 0, errors.New("sandbox daemon resource limits are outside the closed range")
	}
	return maxConnections, maxWireBytes, nil
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func rejectUnsafeSocketPath(path string, contract SocketContract) error {
	parent := filepath.Dir(path)
	info, err := os.Lstat(parent)
	if err != nil {
		return fmt.Errorf("inspect sandbox socket parent: %w", err)
	}
	uid, gid, ok := fileOwner(info)
	productionContractOK := contract.allowUnprivilegedForTest ||
		(contract.ParentUID == 0 && contract.SocketUID == 0 && contract.SocketGID >= 0)
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !ok || !productionContractOK ||
		uid != contract.ParentUID || gid != contract.ParentGID ||
		info.Mode().Perm() != contract.ParentMode.Perm() ||
		contract.ParentMode.Perm()&0o022 != 0 ||
		contract.SocketMode.Perm() != 0o660 {
		return errors.New("sandbox socket parent owner/mode contract differs")
	}
	if !contract.allowUnprivilegedForTest {
		if err := verifyProtectedDirectoryChain(parent, 0); err != nil {
			return fmt.Errorf("sandbox socket parent chain: %w", err)
		}
	}
	if _, err := os.Lstat(path); err == nil {
		return errors.New("sandbox socket path already exists")
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect existing sandbox socket: %w", err)
	}
	return nil
}

func publicErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrDarkFoundation):
		return "dark_foundation"
	case errors.Is(err, ErrPolicy):
		return "policy_rejected"
	case errors.Is(err, context.DeadlineExceeded):
		return "wall_timeout"
	case errors.Is(err, ErrOutputLimit):
		return "output_limit"
	default:
		return "execution_failed"
	}
}

func cleanupSocket(listener *net.UnixListener, path string, original os.FileInfo) error {
	closeErr := listener.Close()
	if errors.Is(closeErr, net.ErrClosed) {
		closeErr = nil
	}
	current, err := os.Lstat(path)
	if err != nil {
		return errors.Join(closeErr, fmt.Errorf("inspect sandbox socket during cleanup: %w", err))
	}
	if !os.SameFile(original, current) {
		return errors.Join(closeErr, errors.New("sandbox socket inode changed before cleanup"))
	}
	return errors.Join(closeErr, os.Remove(path))
}

func fileOwner(info os.FileInfo) (uint32, uint32, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	return stat.Uid, stat.Gid, true
}
