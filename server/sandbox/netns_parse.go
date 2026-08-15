package sandbox

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

func validateRouteTable(reader io.Reader, ipv6 bool) error {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || (!ipv6 && strings.EqualFold(fields[0], "Iface")) {
			continue
		}
		var iface string
		if ipv6 {
			iface = fields[len(fields)-1]
		} else {
			iface = fields[0]
		}
		if iface != "lo" {
			return fmt.Errorf("non-loopback route via %q exists", iface)
		}
	}
	return scanner.Err()
}
