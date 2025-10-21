package manager

import (
	"fmt"
	"log/slog"
	"net"
)

func GetFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer func() {
				if err := l.Close(); err != nil {
					slog.Warn("failed to close TCP listener", "error", err)
				}
			}()
			tcpAddr, ok := l.Addr().(*net.TCPAddr)
			if !ok {
				return 0, fmt.Errorf("failed to get TCP address")
			}
			return tcpAddr.Port, nil
		}
	}
	return
}
