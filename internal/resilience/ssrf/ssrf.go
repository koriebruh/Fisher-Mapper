// Package ssrf provides the shared "is this IP safe to contact" check used
// at two different points around outbound callback delivery
// (internal/domain/payment.CallbackNotifier): once as a best-effort check
// at callback_url validation/create time (reject an obviously-internal
// target before ever persisting it), and once as the AUTHORITATIVE check at
// actual dial time (SafeDialer, wired into the concrete http.Client that
// performs the delivery). The two are deliberately not the same check
// reused verbatim: a hostname's DNS answer can change between create time
// and delivery time (DNS rebinding), so only the dial-time check -- which
// inspects the literal resolved address a connection is about to be made
// to, via net.Dialer.Control -- is actually bypass-proof. The create-time
// check exists purely as defense in depth / fail-fast for the common case.
package ssrf

import (
	"context"
	"fmt"
	"net"
	"syscall"
	"time"
)

// IsBlockedIP reports whether ip must never be dialed by outbound callback
// delivery: loopback (127.0.0.0/8, ::1), link-local unicast/multicast
// (169.254.0.0/16 -- this specifically covers the cloud metadata endpoint
// 169.254.169.254 -- and fe80::/10), private ranges (10/8, 172.16/12,
// 192.168/16, fc00::/7), and unspecified (0.0.0.0, ::).
func IsBlockedIP(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsPrivate() ||
		ip.IsUnspecified()
}

// CheckHost is the create-time, best-effort check: an IP literal is checked
// directly (no network round-trip, always authoritative for that literal);
// a hostname gets a short bounded DNS lookup and is rejected if ANY resolved
// address is blocked (a resolver returning a mix of a decoy public IP and an
// internal one must not let the internal one through). A lookup failure (no
// network, transient DNS issue) is NOT treated as rejection -- this check is
// advisory, not the enforcement boundary; SafeDialer is.
func CheckHost(ctx context.Context, host string) error {
	if ip := net.ParseIP(host); ip != nil {
		if IsBlockedIP(ip) {
			return fmt.Errorf("ssrf: %s is not a publicly routable address", host)
		}
		return nil
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(lookupCtx, host)
	if err != nil {
		return nil
	}
	for _, addr := range addrs {
		if IsBlockedIP(addr.IP) {
			return fmt.Errorf("ssrf: %s currently resolves to a non-public address (%s)", host, addr.IP)
		}
	}
	return nil
}

// SafeDialer returns a *net.Dialer whose Control hook rejects a connection
// attempt against the ACTUAL resolved address it is about to connect(2) to
// -- the only point in the request lifecycle that can't be fooled by DNS
// rebinding (a hostname resolving to a safe IP at check time and an
// internal one at connect time). Wire this into an http.Transport's
// DialContext for any client that dials a caller-supplied URL.
func SafeDialer(timeout time.Duration) *net.Dialer {
	return &net.Dialer{
		Timeout: timeout,
		Control: func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("ssrf: parse dial address %q: %w", address, err)
			}
			ip := net.ParseIP(host)
			if ip == nil {
				return fmt.Errorf("ssrf: dial address %q did not resolve to an IP", address)
			}
			if IsBlockedIP(ip) {
				return fmt.Errorf("ssrf: refusing to dial blocked address %s", ip)
			}
			return nil
		},
	}
}
