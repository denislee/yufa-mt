// Package middleware holds the HTTP middlewares used by the application.
// Each middleware takes its dependencies as explicit arguments so it can
// be unit-tested without package-level state.
package middleware

import (
	"crypto/subtle"
	"fmt"
	"net/http"
)

// BasicAuth wraps handler with HTTP Basic auth against user/pass.
// Constant-time compares prevent leaking the password length via
// timing.
func BasicAuth(user, pass string, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(gotUser), []byte(user)) != 1 ||
			subtle.ConstantTimeCompare([]byte(gotPass), []byte(pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprintln(w, "Unauthorized.")
			return
		}
		handler.ServeHTTP(w, r)
	})
}
