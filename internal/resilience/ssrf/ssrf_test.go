package ssrf

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestIsBlockedIP(t *testing.T) {
	cases := []struct {
		name    string
		ip      string
		blocked bool
	}{
		{"loopback v4", "127.0.0.1", true},
		{"loopback v6", "::1", true},
		{"link-local incl. cloud metadata", "169.254.169.254", true},
		{"link-local v6", "fe80::1", true},
		{"private 10/8", "10.1.2.3", true},
		{"private 172.16/12", "172.16.0.1", true},
		{"private 192.168/16", "192.168.1.1", true},
		{"unique local v6", "fc00::1", true},
		{"unspecified v4", "0.0.0.0", true},
		{"unspecified v6", "::", true},
		// TEST-NET-3, reserved for documentation but not private/loopback/
		// link-local by Go's net.IP classification -- must NOT be blocked.
		{"documentation range treated as public", "203.0.113.5", false},
		{"public IP", "8.8.8.8", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("net.ParseIP(%q) = nil", tc.ip)
			}
			if got := IsBlockedIP(ip); got != tc.blocked {
				t.Errorf("IsBlockedIP(%s) = %v, want %v", tc.ip, got, tc.blocked)
			}
		})
	}
}

func TestCheckHost_IPLiteralFastPath(t *testing.T) {
	if err := CheckHost(context.Background(), "127.0.0.1"); err == nil {
		t.Error("CheckHost(127.0.0.1) = nil, want error")
	}
	if err := CheckHost(context.Background(), "169.254.169.254"); err == nil {
		t.Error("CheckHost(169.254.169.254) = nil, want error")
	}
	if err := CheckHost(context.Background(), "203.0.113.5"); err != nil {
		t.Errorf("CheckHost(203.0.113.5) = %v, want nil", err)
	}
}

func TestCheckHost_LocalhostHostnameRejected(t *testing.T) {
	// Resolves via the local hosts database, not a real DNS round-trip --
	// deterministic without network access.
	if err := CheckHost(context.Background(), "localhost"); err == nil {
		t.Error("CheckHost(localhost) = nil, want error")
	}
}

func TestCheckHost_UnresolvableHostnameDoesNotBlock(t *testing.T) {
	// A lookup failure is advisory-check territory, not authoritative --
	// see CheckHost's doc. Must not reject just because DNS didn't answer.
	err := CheckHost(context.Background(), "this-host-should-not-resolve.invalid")
	if err != nil {
		t.Errorf("CheckHost(unresolvable) = %v, want nil (soft-fail)", err)
	}
}

// TestSafeDialer_RefusesLoopback proves the dial-time hook -- not just the
// create-time CheckHost -- actually refuses to connect to a blocked
// address, exercised via a real net.Listener bound to loopback (the
// authoritative enforcement point this whole package exists for).
func TestSafeDialer_RefusesLoopback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	dialer := SafeDialer(2 * time.Second)
	conn, err := dialer.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err == nil {
		conn.Close()
		t.Fatal("DialContext to loopback listener succeeded, want refused")
	}
}
