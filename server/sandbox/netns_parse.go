package sandbox

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
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
			if len(fields) < 10 {
				return errors.New("malformed IPv6 route entry")
			}
			iface = fields[len(fields)-1]
			loopback := fields[0] == strings.Repeat("0", 31)+"1" && strings.EqualFold(fields[1], "80")
			kernelUnreachableDefault := fields[0] == strings.Repeat("0", 32) && fields[1] == "00" &&
				fields[2] == strings.Repeat("0", 32) && fields[3] == "00" &&
				fields[4] == strings.Repeat("0", 32) && strings.EqualFold(fields[5], "ffffffff") &&
				strings.EqualFold(fields[8], "00200200")
			if !loopback && !kernelUnreachableDefault {
				return fmt.Errorf("non-loopback IPv6 destination %q/%q exists", fields[0], fields[1])
			}
		} else {
			if len(fields) < 8 {
				return errors.New("malformed IPv4 route entry")
			}
			iface = fields[0]
			destination, destinationErr := strconv.ParseUint(fields[1], 16, 32)
			mask, maskErr := strconv.ParseUint(fields[7], 16, 32)
			if destinationErr != nil || maskErr != nil || byte(destination) != 127 || byte(mask) != 255 {
				return fmt.Errorf("non-loopback IPv4 destination %q mask %q exists", fields[1], fields[7])
			}
		}
		if iface != "lo" {
			return fmt.Errorf("non-loopback route via %q exists", iface)
		}
	}
	return scanner.Err()
}
