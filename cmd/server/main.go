// Command server is the Yufa-MT web app entrypoint. It loads .env,
// parses config, and hands off to internal/server.Run. All application
// logic lives in internal/server; this file is config + wiring only.
package main

import (
	"flag"
	"log"
	"os"

	"github.com/joho/godotenv"

	"github.com/denislee/yufa-mt/internal/config"
	"github.com/denislee/yufa-mt/internal/server"
)

func main() {
	// --mode selects the process role (all|app|proxy). Empty = defer to
	// YUFA_MODE / the config default. See docs/two-process-split-plan.md.
	mode := flag.String("mode", "", "process role: all (default), app, or proxy")
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		log.Println("[I] [Main] No .env file found, relying on system environment variables.")
	}
	// An explicit --mode overrides the env before Load so config validation
	// (which is mode-dependent) sees the role the process will actually run.
	if *mode != "" {
		os.Setenv("YUFA_MODE", *mode)
	}
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[F] [Main] %v", err)
	}
	server.Run(cfg)
}
