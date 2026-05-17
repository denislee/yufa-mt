// Command server is the Yufa-MT web app entrypoint. All application
// logic lives in internal/server; this file exists only to satisfy
// `go build` and to keep cmd/ free of business code.
package main

import "github.com/denislee/yufa-mt/internal/server"

func main() { server.Run() }
