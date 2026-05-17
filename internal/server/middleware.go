package server

import (
	"crypto/subtle"
	"fmt"
	"net/http"
)

const adminUser = "admin"

// adminPass is populated by Run from $ADMIN_PASSWORD (or a generated
// random value on first start).
var adminPass string

// basicAuth wraps an admin handler with HTTP Basic auth against
// adminUser/adminPass. Constant-time compares prevent leaking the
// password length via timing.
func basicAuth(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(user), []byte(adminUser)) != 1 ||
			subtle.ConstantTimeCompare([]byte(pass), []byte(adminPass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprintln(w, "Unauthorized.")
			return
		}
		handler.ServeHTTP(w, r)
	})
}
