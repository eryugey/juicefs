/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import (
	"errors"
	"fmt"
	"math/rand"
	"mime"
	"net"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/mattn/go-isatty"
)

// Min returns min of 2 int
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Exists checks if the file/folder in given path exists
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || !os.IsNotExist(err)
}

// SplitDir splits a path with default path list separator or comma.
func SplitDir(d string) []string {
	dd := strings.Split(d, string(os.PathListSeparator))
	if len(dd) == 1 {
		dd = strings.Split(dd[0], ",")
	}
	return dd
}

// GetLocalIp get the local ip used to access remote address.
func GetLocalIp(address string) (string, error) {
	conn, err := net.Dial("udp", address)
	if err != nil {
		return "", err
	}
	ip, _, err := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil {
		return "", err
	}
	return ip, nil
}

func WithTimeout(f func() error, timeout time.Duration) error {
	var done = make(chan int, 1)
	var t = time.NewTimer(timeout)
	var err error
	go func() {
		err = f()
		done <- 1
	}()
	select {
	case <-done:
		t.Stop()
	case <-t.C:
		err = fmt.Errorf("timeout after %s", timeout)
	}
	return err
}

func RemovePassword(uri string) string {
	p := strings.Index(uri, "@")
	if p < 0 {
		return uri
	}
	sp := strings.Index(uri, "://") + 3
	if sp == 2 {
		sp = 0
	}
	cp := strings.Index(uri[sp:], ":")
	if cp < 0 || sp+cp > p {
		return uri
	}
	return uri[:sp+cp] + ":****" + uri[p:]
}

func GuessMimeType(key string) string {
	mimeType := mime.TypeByExtension(path.Ext(key))
	if !strings.ContainsRune(mimeType, '/') {
		mimeType = "application/octet-stream"
	}
	return mimeType
}

func StringContains(s []string, e string) bool {
	for _, item := range s {
		if item == e {
			return true
		}
	}
	return false
}

func FormatBytes(n uint64) string {
	if n < 1024 {
		return fmt.Sprintf("%d Bytes", n)
	}
	units := []string{"K", "M", "G", "T", "P", "E"}
	m := n
	i := 0
	for ; i < len(units)-1 && m >= 1<<20; i++ {
		m = m >> 10
	}
	return fmt.Sprintf("%.2f %siB (%d Bytes)", float64(m)/1024.0, units[i], n)
}

func SupportANSIColor(fd uintptr) bool {
	return isatty.IsTerminal(fd) && runtime.GOOS != "windows"
}

func isExternalIPv4(ip net.IP) bool {
	if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.To4() == nil {
		return false
	}
	return true
}

func GetIpv4() (string, error) {
	// Use ip associated with hostname if possible
	host, err := os.Hostname()
	if err == nil {
		ips, _ := net.LookupIP(host)
		for _, ip := range ips {
			if isExternalIPv4(ip) {
				return ip.String(), nil
			}
		}
	}

	// Iterate over all external IP addresses on all interfaces
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if isExternalIPv4(ip) {
				return ip.String(), nil
			}
		}
	}
	return "", errors.New("cannot find external IPv4 address")
}

func ListenWithRandomPort(ip string) (net.Listener, int, error) {
	i := 0
	rand.Seed(time.Now().UnixNano())
	for ; i < 10; i++ {
		port := rand.Intn(65535-1024) + 1024
		listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
		if err == nil && listener != nil {
			return listener, port, nil
		}
	}
	return nil, -1, errors.New("no available port after 10 attemps")
}
