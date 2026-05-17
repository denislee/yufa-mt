# Yufa-MT

Go web app tracking Project Yufa (Ragnarok Online server) data: market
listings, player counts, characters, guilds, MVP kills, WoE rankings,
in-game chat, and a community trading post fed by a Discord bot.

## Stack

- **Language:** Go 1.24 (`-tags fts5` required for SQLite full-text search)
- **Storage:** SQLite with WAL mode + FTS5 (`mattn/go-sqlite3`)
- **HTTP:** `net/http` standard library, `html/template`
- **Scraping:** `PuerkitoBio/goquery`
- **Packet capture:** `google/gopacket` (libpcap) for in-game chat sniffing
- **Discord:** `bwmarrin/discordgo`
- **AI:** Google Gemini (`google/generative-ai-go`) for parsing trade messages

## Repo layout

```
.
├── cmd/
│   └── server/             # thin main package; calls internal/server.Run
├── internal/
│   ├── server/             # HTTP handlers, scrapers, background jobs, Discord bot
│   ├── storage/            # SQLite open, schema, migrations
│   ├── itemdb/             # YAML item_db_*.yml parser → internal_item_db
│   ├── rms/                # ratemyserver.net / rodatabase.com lookups
│   ├── gemini/             # trade-message → structured items (Gemini API)
│   └── xp/                 # XP tables & calculator math (unit-tested)
├── web/
│   └── templates/          # all .html files served by handlers
├── data/                   # gitignored runtime files (DB, pwd.txt, captures)
│                           # YAML item dumps (item_db_*.yml) are tracked
├── configs/
│   └── .env.example        # env vars the app reads
├── scripts/                # merge.sh, nocomments.sh helpers
├── .github/workflows/      # CI: build, vet, test, golangci-lint
├── Dockerfile              # multi-stage build (CGO + libpcap)
├── RESTRUCTURE_PLAN.md     # phased restructure plan (history & open items)
└── Makefile
```

`cmd/server` only wires things up. All business logic is in `internal/`,
so it's not importable from outside this module — Go's `internal/`
convention enforces that.

## Quick start

```sh
# Local build & run on :8080
make build
./yufa-mt

# Or with the bundled run target (builds, frees port, opens browser):
make run
```

The app opens `data/market_data.db` (created on first run with WAL mode),
hydrates `internal_item_db` from `data/item_db_*.yml`, starts seven
background scrape jobs, attempts a libpcap chat capture, and serves
HTTP on `:8080`.

## Configuration

Copy `configs/.env.example` to `.env` at the repo root and fill in:

| Variable               | Purpose                                                          |
| ---------------------- | ---------------------------------------------------------------- |
| `ADMIN_PASSWORD`       | HTTP Basic password for `/admin/*`. Auto-generated if unset.     |
| `DISCORD_BOT_TOKEN`    | Token for the trading-post Discord bot.                          |
| `DISCORD_CHANNEL_IDS`  | Comma-separated channels the bot listens in.                     |
| `GEMINI_API_KEY`       | Key for the Gemini trade-message parser.                         |
| `CHAT_CAPTURE_DEVICE`  | Network device for libpcap (e.g. `eth0`). Optional.              |
| `CHAT_CAPTURE_PORT`    | Game server TCP port to filter on. Optional.                     |

`ADMIN_PASSWORD` left unset triggers password generation on startup; the
value is logged once and written to `data/pwd.txt` (mode 0600).

## Common tasks

```sh
make build         # build ./cmd/server with -tags fts5
make run           # build + run on :8080, opens browser
make test          # go test -tags fts5 ./...
make vet           # go vet -tags fts5 ./...
make lint          # golangci-lint run (config in .golangci.yml)
make fmt           # go fmt ./...
make tidy          # go mod tidy
make clean         # remove binary + data/pwd.txt
```

## Docker

```sh
docker build -t yufa-mt .
docker run --rm -p 8080:8080 \
  -e ADMIN_PASSWORD=changeme \
  -v "$(pwd)/data:/app/data" \
  yufa-mt
```

The runtime image is Debian slim because both `mattn/go-sqlite3` (CGO)
and `gopacket/pcap` (libpcap) need shared libraries; distroless won't
work here. Mount `data/` so the SQLite DB persists across container
restarts.

## Architecture

- **Request path:** `cmd/server` builds an `http.ServeMux` via
  `internal/server.registerRoutes`, then wraps every public route in
  the `visitorTracker` middleware which batches page views into the
  `page_views` table (10 s ticker, 100 row batch, drops on backpressure).
  Admin routes go through `basicAuth`.
- **Background jobs:** `scraper_jobs.go` schedules each scraper on its
  own ticker via `runJobOnTicker`. All jobs receive the same `ctx`
  cancelled by SIGINT/SIGTERM, so shutdown drains in-flight work.
- **Storage:** `internal/storage.Open` creates schema + indexes + a
  dynamic MVP-kills table (one column per mob ID passed in by the
  caller), then runs idempotent migrations. WAL mode + 10 conn pool
  allows parallel reads while a single writer holds the lock.
- **i18n:** Translations live in `internal/server/i18n.go` as a
  `map[lang]map[key]string`; the language cookie (`pt` default) selects
  the active map. New keys go in both `en` and `pt`.
- **Templates:** Cached at package init from `web/templates/*.html`.
  Each page template is parsed alongside `navbar.html` and
  `pagination.html`. Render dispatch is `renderTemplate(w, r, name, data)`.

## Testing

```sh
go test -tags fts5 ./...
```

Pure-math packages (`internal/xp`) have unit tests. The server package
mostly drives I/O and remains untested; see Phase 10 in
`RESTRUCTURE_PLAN.md` for the path to repository interfaces that would
enable broader handler tests.

## Restructure history

This repo was migrated from a flat `package main` (12.6k LOC across 12
root files) to the layout above. The phased plan and decision log live
in `RESTRUCTURE_PLAN.md`.
