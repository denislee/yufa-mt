// Command server is the Yufa-MT web app entrypoint. It loads .env,
// parses config, and hands off to internal/server.Run. All application
// logic lives in internal/server; this file is config + wiring only.
package main

import (
	"log"

	"github.com/joho/godotenv"

	"github.com/denislee/yufa-mt/internal/config"
	"github.com/denislee/yufa-mt/internal/server"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[I] [Main] No .env file found, relying on system environment variables.")
	}
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[F] [Main] %v", err)
	}
	server.Run(cfg)
}
