package sspanelruntime

import (
	"net"
	"strings"
	"sync"
)

type Hook interface {
	RecordAliveIP(email string, ip string)
	ShouldReject(email string, ip string) bool
}

var (
	access     sync.RWMutex
	activeHook Hook
)

func SetHook(h Hook) {
	access.Lock()
	activeHook = h
	access.Unlock()
}

func ClearHook(h Hook) {
	access.Lock()
	if activeHook == h {
		activeHook = nil
	}
	access.Unlock()
}

func RecordAliveIP(email string, remoteAddr string) {
	access.RLock()
	hook := activeHook
	access.RUnlock()
	if hook == nil {
		return
	}
	ip := remoteIP(remoteAddr)
	if ip == "" {
		return
	}
	hook.RecordAliveIP(email, ip)
}

func ShouldReject(email string, remoteAddr string) bool {
	access.RLock()
	hook := activeHook
	access.RUnlock()
	if hook == nil {
		return false
	}
	ip := remoteIP(remoteAddr)
	if ip == "" {
		return false
	}
	return hook.ShouldReject(email, ip)
}

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(remoteAddr, "[]")
}
