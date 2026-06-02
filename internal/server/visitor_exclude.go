package server

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
	"time"
)

// Excluding the operator's own browsing from the visitor stats works two
// ways, both handled here:
//
//   - Cookie: after a successful admin BasicAuth, setNoTrackCookie drops a
//     signed "no-track" cookie. setNoTrackCookie is mounted INSIDE the
//     BasicAuth wrapper, so the cookie is only ever issued to an
//     authenticated admin. The cookie is Path=/, so the browser sends it
//     back on the public pages too, where skipVisitorLog matches it and
//     drops the page view. This is device-agnostic: log into /admin once
//     from a browser and that browser stops counting.
//   - IP: any source IP listed in VISITOR_IGNORE_IPS is skipped regardless
//     of cookie. Handy for a fixed home/office IP.
//
// The cookie value is derived from the admin password so a random visitor
// can't guess it and opt themselves out. If ADMIN_PASSWORD is unset and a
// random one is generated per boot, the token changes each restart and the
// operator must revisit /admin once to refresh the cookie.

const noTrackCookie = "yufa_nt"

var (
	noTrackToken string
	ignoreIPSet  map[string]struct{}
)

// initVisitorExclusion derives the cookie token, builds the IP allowlist,
// and installs the skip predicate on the visitor logger. Must run after
// the admin password is finalized and before the server starts serving.
func initVisitorExclusion(adminPass string, ignoreIPs []string) {
	sum := sha256.Sum256([]byte("yufa-notrack:" + adminPass))
	noTrackToken = hex.EncodeToString(sum[:])

	ignoreIPSet = make(map[string]struct{}, len(ignoreIPs))
	for _, ip := range ignoreIPs {
		if ip = strings.TrimSpace(ip); ip != "" {
			ignoreIPSet[ip] = struct{}{}
		}
	}

	visitorLogger.SetSkip(skipVisitorLog)
}

// skipVisitorLog reports whether a request is the operator's own and so
// should be left out of the visitor stats.
func skipVisitorLog(r *http.Request) bool {
	if c, err := r.Cookie(noTrackCookie); err == nil && c.Value == noTrackToken {
		return true
	}
	if len(ignoreIPSet) > 0 {
		if _, ok := ignoreIPSet[clientIP(r)]; ok {
			return true
		}
	}
	return false
}

// clientIP mirrors the IP extraction used for the visitor hash (X-Forwarded-For
// first hop, else RemoteAddr) so VISITOR_IGNORE_IPS matches the same address.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}

// setNoTrackCookie issues the no-track cookie to authenticated admins. It is
// wrapped inside BasicAuth, so it only runs once the request is a verified
// admin. Once set, that browser's public-page visits are excluded by
// skipVisitorLog.
func setNoTrackCookie(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c, err := r.Cookie(noTrackCookie); err != nil || c.Value != noTrackToken {
			http.SetCookie(w, &http.Cookie{
				Name:     noTrackCookie,
				Value:    noTrackToken,
				Path:     "/",
				MaxAge:   int((365 * 24 * time.Hour).Seconds()),
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
			})
		}
		next.ServeHTTP(w, r)
	})
}
