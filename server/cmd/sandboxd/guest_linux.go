//go:build linux

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const guestReceiptMarker = "VANE_FIRECRACKER_RECEIPT="

type guestInitEnvironment struct {
	pid         int
	mkdirAll    func(string, os.FileMode) error
	mount       func(string, string, string, uintptr, string) error
	readCmdline func() ([]byte, error)
	openInput   func() (*os.File, error)
	openTTY     func() (*os.File, error)
	runWorker   func(*os.File, *os.File, []string, uint32, uint32) error
	sync        func()
	reboot      func() error
}

func productionGuestInitEnvironment() guestInitEnvironment {
	return guestInitEnvironment{
		pid: os.Getpid(), mkdirAll: os.MkdirAll, mount: unix.Mount,
		readCmdline: func() ([]byte, error) { return os.ReadFile("/proc/cmdline") },
		openInput:   func() (*os.File, error) { return os.Open("/dev/vdb") },
		openTTY:     func() (*os.File, error) { return os.OpenFile("/dev/ttyS0", os.O_WRONLY, 0) },
		runWorker: func(input, tty *os.File, arguments []string, uid, gid uint32) error {
			command := exec.Command("/sbin/vane-sandbox-init", arguments...)
			command.ExtraFiles = []*os.File{input}
			command.Stdout, command.Stderr = tty, tty
			command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: gid}}
			return command.Run()
		},
		sync: unix.Sync, reboot: func() error { return unix.Reboot(unix.LINUX_REBOOT_CMD_RESTART) },
	}
}

func runGuest(arguments []string, stdout, stderr io.Writer) int {
	if len(arguments) > 0 && arguments[0] == "guest-worker" {
		return runGuestWorker(arguments[1:], stdout, stderr)
	}
	return runGuestInit(arguments, stderr, productionGuestInitEnvironment())
}

func runGuestInit(arguments []string, stderr io.Writer, environment guestInitEnvironment) int {
	if len(arguments) != 0 || environment.pid != 1 {
		fmt.Fprintln(stderr, "sandbox guest init rejected its execution context")
		return 1
	}
	for _, directory := range []string{"/proc", "/sys", "/dev"} {
		if err := environment.mkdirAll(directory, 0o755); err != nil {
			fmt.Fprintln(stderr, "sandbox guest init failed")
			return 1
		}
	}
	if err := environment.mount("proc", "/proc", "proc", uintptr(unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC), ""); err != nil {
		fmt.Fprintln(stderr, "sandbox guest proc mount failed")
		return 1
	}
	// The initramfs contains only the /dev mountpoint. Firecracker's virtio
	// block and serial devices are populated by devtmpfs after the kernel has
	// discovered them, so PID 1 must mount it before opening /dev/vdb or
	// /dev/ttyS0. MS_NODEV is intentionally absent: this is the guest's private
	// device filesystem and those device nodes are the fixed I/O contract.
	if err := environment.mount("devtmpfs", "/dev", "devtmpfs", uintptr(unix.MS_NOSUID|unix.MS_NOEXEC), "mode=0755"); err != nil {
		fmt.Fprintln(stderr, "sandbox guest devtmpfs mount failed")
		return 1
	}
	if err := environment.mount("sysfs", "/sys", "sysfs", uintptr(unix.MS_RDONLY|unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC), ""); err != nil {
		fmt.Fprintln(stderr, "sandbox guest sysfs mount failed")
		return 1
	}
	commandLine, err := environment.readCmdline()
	if err != nil {
		fmt.Fprintln(stderr, "sandbox guest cmdline unavailable")
		return 1
	}
	values := map[string]string{}
	for _, field := range strings.Fields(string(commandLine)) {
		if key, value, ok := strings.Cut(field, "="); ok && strings.HasPrefix(key, "vane.") {
			if _, exists := values[key]; exists {
				fmt.Fprintln(stderr, "sandbox guest cmdline duplicated")
				return 1
			}
			values[key] = value
		}
	}
	required := []string{"vane.input_bytes", "vane.input_sha256", "vane.request_sha256", "vane.invocation", "vane.uid", "vane.gid"}
	for _, key := range required {
		if values[key] == "" {
			fmt.Fprintln(stderr, "sandbox guest cmdline incomplete")
			return 1
		}
	}
	uid, uidErr := strconv.Atoi(values["vane.uid"])
	gid, gidErr := strconv.Atoi(values["vane.gid"])
	inputBytes, inputErr := strconv.Atoi(values["vane.input_bytes"])
	if uidErr != nil || gidErr != nil || inputErr != nil || uid <= 0 || gid <= 0 || inputBytes < 1 || inputBytes > 64<<10 {
		fmt.Fprintln(stderr, "sandbox guest authority limits invalid")
		return 1
	}
	input, err := environment.openInput()
	if err != nil {
		fmt.Fprintln(stderr, "sandbox guest input device unavailable")
		return 1
	}
	tty, err := environment.openTTY()
	if err != nil {
		fmt.Fprintln(stderr, "sandbox guest serial unavailable")
		return 1
	}
	workerArguments := []string{"guest-worker", values["vane.input_bytes"], values["vane.input_sha256"],
		values["vane.request_sha256"], values["vane.invocation"], values["vane.uid"], values["vane.gid"]}
	err = environment.runWorker(input, tty, workerArguments, uint32(uid), uint32(gid))
	_ = input.Close()
	_ = tty.Close()
	if err != nil {
		fmt.Fprintln(stderr, "sandbox guest worker failed")
		return 1
	}
	environment.sync()
	if err := environment.reboot(); err != nil {
		fmt.Fprintln(stderr, "sandbox guest reboot failed")
		return 1
	}
	return 0
}

func runGuestWorker(arguments []string, stdout, stderr io.Writer) int {
	inputFile := os.NewFile(3, "input.raw")
	return runGuestWorkerWithEnvironment(arguments, stdout, stderr, os.Geteuid(), os.Getegid(), inputFile, inspectGuestNetwork)
}

func runGuestWorkerWithEnvironment(arguments []string, stdout, stderr io.Writer, effectiveUID, effectiveGID int,
	inputFile io.Reader, inspectNetwork func() (bool, bool)) int {
	if len(arguments) != 6 || effectiveUID <= 0 || effectiveGID <= 0 {
		fmt.Fprintln(stderr, "sandbox guest worker authority invalid")
		return 1
	}
	inputBytes, inputErr := strconv.Atoi(arguments[0])
	wantUID, uidErr := strconv.Atoi(arguments[4])
	wantGID, gidErr := strconv.Atoi(arguments[5])
	if inputErr != nil || uidErr != nil || gidErr != nil || inputBytes < 1 || inputBytes > 64<<10 || effectiveUID != wantUID || effectiveGID != wantGID {
		fmt.Fprintln(stderr, "sandbox guest worker limits invalid")
		return 1
	}
	if inputFile == nil {
		fmt.Fprintln(stderr, "sandbox guest worker input missing")
		return 1
	}
	input := make([]byte, inputBytes)
	if _, err := io.ReadFull(inputFile, input); err != nil {
		fmt.Fprintln(stderr, "sandbox guest worker input truncated")
		return 1
	}
	inputDigest := sha256.Sum256(input)
	if hex.EncodeToString(inputDigest[:]) != arguments[1] {
		fmt.Fprintln(stderr, "sandbox guest worker input digest mismatch")
		return 1
	}
	onlyLoopback, noDefaultRoute := inspectNetwork()
	response := sha256.Sum256(append([]byte("vane-firecracker-self-test/v1\x00"), input...))
	receipt := struct {
		Schema          string `json:"schema"`
		Invocation      string `json:"invocation_id"`
		Request         string `json:"request_sha256"`
		Input           string `json:"input_sha256"`
		Response        string `json:"response_sha256"`
		GuestUID        int    `json:"guest_uid"`
		GuestGID        int    `json:"guest_gid"`
		OnlyLoopback    bool   `json:"only_loopback"`
		NoDefaultRoute  bool   `json:"no_default_route"`
		MMDSUnavailable bool   `json:"mmds_unavailable"`
	}{"vane.firecracker-guest-receipt/v1", arguments[3], arguments[2], arguments[1],
		hex.EncodeToString(response[:]), wantUID, wantGID, onlyLoopback, noDefaultRoute,
		onlyLoopback && noDefaultRoute}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return 1
	}
	_, err = fmt.Fprintf(stdout, "%s%s\n", guestReceiptMarker, payload)
	if err != nil {
		return 1
	}
	return 0
}

func inspectGuestNetwork() (bool, bool) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return false, false
	}
	return inspectGuestNetworkWithEnvironment(interfaces, func(path string) ([]byte, error) {
		return os.ReadFile(filepath.Clean(path))
	})
}

func inspectGuestNetworkWithEnvironment(interfaces []net.Interface, readFile func(string) ([]byte, error)) (bool, bool) {
	if len(interfaces) != 1 || interfaces[0].Name != "lo" || interfaces[0].Flags&net.FlagLoopback == 0 {
		return false, false
	}
	for index, path := range []string{"/proc/net/route", "/proc/net/ipv6_route"} {
		payload, err := readFile(path)
		if err != nil {
			return true, false
		}
		for lineIndex, line := range strings.Split(string(payload), "\n") {
			if (index == 0 && lineIndex == 0) || strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Fields(line)
			ipv4Default := index == 0 && len(fields) > 1 && fields[1] == "00000000"
			ipv6Default := index == 1 && len(fields) > 8 && fields[0] == strings.Repeat("0", 32) && fields[1] == "00" &&
				!(fields[2] == strings.Repeat("0", 32) && fields[3] == "00" && fields[4] == strings.Repeat("0", 32) &&
					strings.EqualFold(fields[5], "ffffffff") && strings.EqualFold(fields[8], "00200200"))
			if ipv4Default || ipv6Default {
				return true, false
			}
		}
	}
	return true, true
}
