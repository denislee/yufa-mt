package server

// adminUser and adminPass are populated by Run from the loaded config
// (or a generated random password on first start). basicAuth is kept as
// a thin shim into internal/middleware so existing call sites in
// server.go don't need updating.
var (
	adminUser = "admin"
	adminPass string
)
