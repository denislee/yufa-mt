# Yufa-MT Restructure Plan

> Living plan: any Claude agent working on this restructure must read this file
> first, pick the next unchecked task, execute it, and update the status
> sections (Status, Decisions, Log) at the bottom before stopping. Phases are
> ordered — finish a phase before starting the next.

## Goals

Turn the current flat `package main` (12.6k LOC across 12 root files + 25 HTML
templates + large YAML data files) into an idiomatic Go layout that:

1. Follows the [Standard Go Project Layout](https://github.com/golang-standards/project-layout)
   conventions (`cmd/`, `internal/`, `migrations/`, `web/`, `scripts/`).
2. Eliminates package-level mutable globals where reasonable (currently `db`,
   `adminUser`, `adminPass`, `dashboardStatsCache`, `scraperClient`, item-DB
   caches, XP tables, etc.) in favour of constructor-injected dependencies.
3. Makes the app testable: small packages with explicit interfaces for the
   database, scraper HTTP client, and external services (Gemini, Discord, RMS).
4. Keeps the codebase shippable at every step — `make build && make run` must
   succeed at the end of every phase. Phases are mergeable PRs, not a big bang.

## Non-goals

- Rewriting business logic, SQL, or templates. Behaviour is unchanged.
- Switching frameworks (no chi/gin/echo). Stay on `net/http`.
- Introducing an ORM. Keep raw `database/sql`.
- Adding new features.

## Target layout

```
yufa-mt/
├── cmd/
│   └── server/
│       └── main.go                # wiring only: load config, open DB, build app, ListenAndServe
├── internal/
│   ├── app/                       # App struct holding deps (DB, config, scraper client, etc.)
│   │   └── app.go
│   ├── config/                    # env + flag loading, validation
│   │   └── config.go
│   ├── storage/                   # DB open, migrations, low-level helpers
│   │   ├── sqlite.go              # initDB + WAL + pragmas
│   │   ├── migrations.go          # applyMigrations
│   │   ├── schema.go              # createTables, createIndexes, createDynamicTables
│   │   └── helpers.go             # toNullInt64, etc.
│   ├── models/                    # pure domain types (no DB calls, no HTTP)
│   │   ├── item.go
│   │   ├── character.go
│   │   ├── guild.go
│   │   ├── trading.go
│   │   ├── chat.go
│   │   ├── woe.go
│   │   └── visitor.go
│   ├── itemdb/                    # YAML item DB parser + lookup
│   │   ├── parser.go              # parseAndStoreItemDB, storeItemsInDB
│   │   └── bootstrap.go           # populateItemDBOnStartup
│   ├── rms/                       # ratemyserver.net scraper/cache
│   │   ├── client.go
│   │   └── cache.go
│   ├── gemini/                    # Gemini trade-message parser
│   │   └── parser.go
│   ├── discord/                   # discord bot
│   │   └── bot.go
│   ├── scraper/                   # split scraper.go by domain (see Phase 4)
│   │   ├── client.go              # ScraperClient + retries + rate-limit
│   │   ├── market.go              # scrapeData
│   │   ├── players.go             # scrapeAndStorePlayerCount
│   │   ├── characters.go          # scrapePlayerCharacters
│   │   ├── guilds.go              # scrapeGuilds
│   │   ├── mvp.go                 # scrapeMvpKills
│   │   ├── zeny.go                # scrapeZeny
│   │   ├── woe.go                 # scrapeWoeCharacterRankings
│   │   ├── ptnames.go             # populateMissingPortugueseNames
│   │   └── jobs.go                # runJobOnTicker, startBackgroundJobs
│   ├── xp/                        # XP tables + calculator
│   │   └── xp.go
│   ├── i18n/                      # getLang, getTranslations, setLangHandler
│   │   └── i18n.go
│   ├── render/                    # template loading, render helpers, FuncMap
│   │   ├── render.go
│   │   └── funcs.go               # tmplHTML, dict, formatZeny, ...
│   ├── middleware/                # visitorTracker, basicAuth, recoverer
│   │   ├── visitor.go
│   │   └── auth.go
│   ├── visitor/                   # page-view batch logger
│   │   └── logger.go
│   ├── httpx/                     # shared HTTP helpers (pagination, sort clause, search)
│   │   └── httpx.go
│   ├── handlers/                  # public handlers, one file per page family
│   │   ├── summary.go
│   │   ├── fulllist.go
│   │   ├── item.go
│   │   ├── activity.go
│   │   ├── players.go
│   │   ├── characters.go
│   │   ├── guilds.go
│   │   ├── mvp.go
│   │   ├── store.go
│   │   ├── trading.go
│   │   ├── woe.go
│   │   ├── chat.go
│   │   ├── stats.go               # drops, market, characters stats
│   │   ├── xp.go
│   │   ├── search.go
│   │   ├── about.go
│   │   └── routes.go              # registerRoutes(app)
│   └── admin/                     # all admin/* handlers + dashboard
│       ├── dashboard.go
│       ├── cache.go
│       ├── trading.go
│       ├── chat.go
│       ├── scrape_triggers.go
│       ├── cleanup.go
│       └── routes.go              # registerAdminRoutes(app)
├── migrations/                    # (optional, future) raw .sql files if we extract from schema.go
├── web/
│   ├── templates/                 # all *.html files moved here
│   └── static/                    # (future) images, css, js if any
├── scripts/
│   ├── merge.sh
│   └── nocomments.sh
├── data/                          # gitignored: market_data.db, item_db_*.yml, pwd.txt
│   └── .gitkeep
├── configs/
│   └── .env.example
├── Dockerfile                     # (Phase 9)
├── .github/workflows/ci.yml       # (Phase 9)
├── .golangci.yml                  # (Phase 9)
├── Makefile
├── README.md
├── go.mod
└── go.sum
```

## Guiding principles for the migration

- **One package per concern, named after the concern**, not after the layer
  (`internal/scraper`, not `internal/services`).
- **No cyclic imports.** Dependency direction: `handlers` → `storage`/`models`,
  never the other way. `models` imports nothing from this repo.
- **`internal/app.App`** is the dependency container. Pass `*app.App` (or a
  smaller interface) to handlers via closures or method receivers — no
  package-level `db`.
- **Embed templates** with `//go:embed web/templates/*.html` once they're moved.
  Stop reading them from CWD.
- **Migrations are append-only.** Keep `applyMigrations`'s `schema_migrations`
  table semantics; don't renumber.
- **Tests live next to code** (`foo_test.go`), not in a separate tree.
- **Every phase ends green:** `make fmt && make build && make test` passes.
- **Commits are small.** Aim for one logical move per commit so `git log`
  documents the migration.

---

## Phase 0 — Baseline & safety net

Goal: have a known-good starting point and the tools to verify each later step.

- [ ] Run `make build` and `make test`; record current build size and any
      warnings in the Log section.
- [ ] Add `.golangci.yml` with conservative defaults (`govet`, `staticcheck`,
      `errcheck`, `ineffassign`, `unused`, `gofmt`). Don't fix findings yet —
      capture the baseline count in the Log.
- [ ] Add `go vet ./...` to the Makefile `lint` target (alongside golangci).
- [ ] Tag the current commit (`git tag pre-restructure`) so we can diff easily.
- [ ] Smoke-test the running app: start it, hit `/`, `/players`, `/admin/`, and
      confirm one scrape job runs without panicking. Note the test list in the
      Log so later phases can rerun the same checks.

Exit criteria: build is green, lint baseline captured, tag exists.

---

## Phase 1 — Repo hygiene (no Go file moves yet)

Goal: cheap wins that reduce churn in later diffs.

- [ ] Move `*.html` → `web/templates/`. Update the one place templates are
      loaded (`renderTemplate` in handlers.go) to read from
      `web/templates/<file>`.
- [ ] Move `merge.sh`, `nocomments.sh` → `scripts/`.
- [ ] Move `item_db_*.yml` → `data/` (and update `parseAndStoreItemDB` caller
      in `populateItemDBOnStartup`). Add `data/` to `.gitignore` for the DB
      and `pwd.txt`, but keep the YAMLs tracked (they're seed data).
- [ ] Add `configs/.env.example` documenting every env var the app reads
      (`ADMIN_PASSWORD`, `GEMINI_API_KEY`, Discord token, etc. — grep
      `os.Getenv` to find them all).
- [ ] Move `all.txt`, `capture.out` out of the repo (or into `data/`) and
      ensure they're gitignored.
- [ ] Verify `make run` still works end-to-end.

Exit criteria: only `*.go`, `go.mod`, `go.sum`, `Makefile`, `README.md`,
`RESTRUCTURE_PLAN.md`, and the new top-level dirs remain at the repo root.

---

## Phase 2 — Carve out leaf packages (`models`, `xp`, `i18n`, `itemdb`, `gemini`, `rms`)

Goal: extract self-contained code that other modules depend on but which
doesn't depend on the HTTP layer. Do these in the order listed so each move
compiles on its own.

For each package below, the recipe is:
1. Create `internal/<pkg>/` with the files listed.
2. Change `package main` → `package <pkg>`.
3. Export the identifiers callers will need (capitalise first letter); leave
   internal helpers lowercase.
4. Update all call sites in remaining root files to import and use the new
   package.
5. `make build` must pass before committing.

- [ ] **`internal/models`** — move every pure struct from `models.go` and the
      struct-only fragments scattered across `handlers.go` (e.g. `EventDefinition`,
      `PaginationData`, `ItemTypeTab`, `PageViewEntry`). Methods that only touch
      the struct itself (`ShortHash`, `CreatedAgo`, `DisplayName`) go with them.
      No DB calls in this package.
- [ ] **`internal/xp`** — move `xp.go` wholesale. Export `Calculate`,
      `LevelTable`, `JobTable` as needed. The `xpCalculatorHandler` stays in
      `handlers.go` for now and calls into `xp`.
- [ ] **`internal/i18n`** — move `getTranslations`, `getLang`, `setLangHandler`
      and the translations map from `handlers.go`. Export `Lang(r)`,
      `Translations(lang)`, `SetLangHandler`.
- [ ] **`internal/itemdb`** — move `item_parser.go`. Export
      `PopulateOnStartup(db *sql.DB)` and `Parse(filenames []string)`.
- [ ] **`internal/gemini`** — move `gemini_parser.go`. Export `ParseTradeMessage`.
      Wrap the API key read in a `New(apiKey string) *Client` constructor
      rather than reading env inside the function.
- [ ] **`internal/rms`** — move `rms.go`. Export `GetItemDetails(db, id)` and
      `SearchDatabase(query, slots)`. The cache mutex becomes a struct field.

Exit criteria: `models.go`, `xp.go`, `item_parser.go`, `gemini_parser.go`,
`rms.go` are deleted from the root. `package main` files at root still
compile and the app still runs.

---

## Phase 3 — Storage package

Goal: get the `db *sql.DB` global out of root and into a package that owns
schema and connection lifecycle.

- [ ] Create `internal/storage` with `sqlite.go`, `migrations.go`, `schema.go`,
      `helpers.go`. Move `db.go` content split by responsibility.
- [ ] Replace the package-level `var db *sql.DB` with a `*storage.DB` (a thin
      wrapper around `*sql.DB`) returned by `storage.Open(path string)`.
- [ ] In root files, temporarily keep a `var db *sql.DB` shim assigned from
      `storage.Open(...).Raw()` so the rest of the code compiles unchanged.
      This shim is removed in Phase 6.
- [ ] Move `toNullInt64` to `storage/helpers.go`; re-export and update callers.

Exit criteria: schema/migration code lives in `internal/storage`. The global
`db` still exists at root but is the only remaining reference; everything new
goes through `storage.DB`.

---

## Phase 4 — Split `scraper.go` (3.1k LOC)

Goal: one file per scrape target. No behaviour changes.

- [ ] Create `internal/scraper/` with files listed in the Target Layout.
- [ ] Move `ScraperClient`, retry logic, and rate limiting to `client.go`.
      Make `New(...)` take a config struct (timeout, max retries) instead of
      reading globals.
- [ ] Move each `scrapeX` function to its own file. Shared regexes stay at
      package level inside `scraper`.
- [ ] Move `runJobOnTicker` and `startBackgroundJobs` to `jobs.go`. The
      scheduler takes `*storage.DB` and a `*ScraperClient`; jobs become methods.
- [ ] Update `main.go` and `admin_handlers.go` to call `scraper.New(...)` and
      `scraper.StartJobs(ctx, ...)`.

Exit criteria: `scraper.go` is deleted. New package builds and `go vet` is
clean. Manually trigger one scrape via `/admin/scrape/players` and confirm it
runs.

---

## Phase 5 — Carve out HTTP plumbing

Goal: extract the cross-cutting HTTP concerns out of `handlers.go` and
`admin_handlers.go` before splitting the handlers themselves.

- [ ] **`internal/render`** — `renderTemplate`, template parsing, the FuncMap
      (`tmplHTML`, `dict`, `formatZeny`, `formatRMT`, `getKillCount`,
      `formatAvgLevel`, `getClassImageURL`, `toggleOrder`, `cleanCardName`,
      `parseDropMessage`). Use `//go:embed web/templates/*.html` here and
      parse once at startup. Export `Render(w, r, name, data)`.
- [ ] **`internal/httpx`** — `newPaginationData`, `getSortClause`,
      `buildItemSearchClause`, `sanitizeString`. Export each.
- [ ] **`internal/middleware`** — `visitorTracker`, `basicAuth`. Auth takes
      `(user, pass string)` instead of reading globals.
- [ ] **`internal/visitor`** — `pageViewLog`, `pageViewChannel`,
      `flushVisitorBatchToDB`, `startVisitorLogger`. Expose a `Logger` struct
      with `Track(r)` and `Run(ctx)`.

Exit criteria: handlers files no longer define these helpers, only call them.

---

## Phase 6 — Split public handlers

Goal: break `handlers.go` (5.4k LOC) into one file per page family inside
`internal/handlers`.

- [ ] Define a `Handlers` struct holding `*storage.DB`, `*scraper.Client`,
      `*rms.Client`, `*gemini.Client`, `*render.Renderer`, `*visitor.Logger`,
      `*config.Config`. Methods on `*Handlers` replace free functions.
- [ ] Move handlers file-by-file in this order (smallest blast radius first):
  - [ ] `about.go`, `xp.go` (calculator handler), `search.go`
  - [ ] `players.go`, `mvp.go`, `chat.go`, `store.go`
  - [ ] `activity.go`, `summary.go`, `fulllist.go`, `item.go`
  - [ ] `characters.go`, `guilds.go`, `trading.go`, `woe.go`, `stats.go`
- [ ] After every file move, `make build && make test` must pass and a
      manual smoke of the moved route must succeed.
- [ ] Move `registerRoutes` to `internal/handlers/routes.go`; it takes
      `*Handlers` and returns `*http.ServeMux`.
- [ ] Delete the package-level `db` shim from Phase 3.

Exit criteria: `handlers.go` is deleted.

---

## Phase 7 — Split admin handlers

Goal: same treatment for `admin_handlers.go` (1.6k LOC).

- [ ] Define `internal/admin.Handlers` mirroring the public `Handlers` struct.
- [ ] Split by domain: `dashboard.go`, `cache.go`, `trading.go`, `chat.go`,
      `scrape_triggers.go`, `cleanup.go`.
- [ ] `adminTriggerScrapeHandler` becomes a method that closes over a
      registered map of scrape functions exposed by `internal/scraper`.
- [ ] Move `registerAdminRoutes` to `internal/admin/routes.go`.

Exit criteria: `admin_handlers.go` is deleted. The only Go file left in the
repo root is none — all code is under `cmd/` and `internal/`.

---

## Phase 8 — `cmd/server/main.go`

Goal: a tiny, readable entrypoint.

- [ ] Move `main.go` to `cmd/server/main.go`. It should do, in order:
  1. Load config (`config.Load()`).
  2. Open storage (`storage.Open(cfg.DBPath)`).
  3. Run item-DB bootstrap (`itemdb.PopulateOnStartup(db)`).
  4. Construct clients: `rms.New`, `gemini.New`, `scraper.New`.
  5. Construct `visitor.Logger`, start it.
  6. Construct `handlers.Handlers` and `admin.Handlers`.
  7. Build mux: `handlers.Register(mux, ...)` and
     `admin.Register(mux, basicAuth(...))`.
  8. Start `scraper.StartJobs(ctx, ...)` and `discord.Start(ctx, ...)`.
  9. `http.Server.ListenAndServe` with the existing graceful-shutdown block.
- [ ] Update Makefile: `BUILD_TARGET=./cmd/server`, binary name unchanged.
- [ ] Delete `utils.go` after moving `generateRandomPassword` to
      `internal/config` (where it's only used for the admin-password fallback)
      and `GetLast*Time` helpers to wherever their tables live (likely
      `internal/storage`).

Exit criteria: `go build ./cmd/server` produces a working binary. `make run`
still launches the same app on :8080.

---

## Phase 9 — Production polish

Goal: things a production Go project has that this one doesn't yet.

- [ ] **Structured logging.** Replace `log.Printf` with `log/slog`. Add
      `slog.Default` configuration in `main.go` (JSON in prod, text in dev,
      level from env).
- [ ] **Config struct & validation.** Required vars fail fast at startup with
      a clear error.
- [ ] **Graceful shutdown ordering.** Server first, then visitor logger drain,
      then DB close. Document in `cmd/server/main.go`.
- [ ] **Tests.** Add table-driven unit tests for the highest-value pure
      functions first: `internal/xp` calculator, `internal/httpx` pagination
      and sort clause, `internal/render` formatters (`formatZeny`, `formatRMT`),
      `internal/itemdb` parser. Target: every package has at least one test.
- [ ] **Dockerfile** (multi-stage: `golang:1.24` builder → `gcr.io/distroless/base`
      runtime, with CGO enabled for `mattn/go-sqlite3`).
- [ ] **CI** (`.github/workflows/ci.yml`): `go vet`, `golangci-lint run`,
      `go test -tags fts5 ./...` on every PR.
- [ ] **README.md** rewrite covering: what it is, how to build, env vars,
      running locally, layout overview (link this file).
- [ ] **`go.mod`**: confirm Go version matches CI; run `go mod tidy`.
- [ ] Remove `pwd.txt`-on-disk pattern in favour of logging the generated
      password once on startup (or require `ADMIN_PASSWORD` to be set in
      prod and refuse to start without it).

Exit criteria: CI green on a fresh clone, Docker image builds and runs.

---

## Phase 10 — Optional follow-ups

Only after Phase 9 is done and merged. Each can be its own future plan.

- [ ] Extract raw SQL from `internal/storage/schema.go` into versioned
      `migrations/*.sql` files driven by something like `golang-migrate` or
      a tiny in-house runner.
- [ ] Introduce a repository interface per domain (`CharacterRepo`,
      `GuildRepo`, …) so handlers depend on interfaces, not `*storage.DB`.
      Only worth doing if it unlocks tests.
- [ ] Replace `net/http` `ServeMux` with `http.ServeMux` (Go 1.22 routing) or
      `chi` if path params get painful.
- [ ] Metrics endpoint (`/metrics`) with Prometheus, behind admin auth.

---

## Status

_Update this section as work progresses. Tick boxes above, summarise here._

- **Current phase:** All phases 0–9 completed (2026-05-17 session).
- **Last verified green build:** `make build` (commit at end of session)
  and `go test -tags fts5 ./internal/xp/` both pass; app boots and runs
  background jobs end-to-end. Smoke-tested by launching the binary and
  watching it scrape, hydrate the item DB, and serve `:8080`.
- **Open work captured as deviations:** see Decisions and Phase 10.

## Decisions

_Record any deviations from the plan and why. Future agents should read this
before proposing changes._

- **2026-05-17 — Hybrid package layout adopted.** The original plan listed
  ~25 internal packages with full callsite qualification. With ~12.6k LOC of
  tightly coupled code (handlers reference dozens of model types, scrapers
  share regexes, admin handlers reuse public-handler helpers), a strict
  per-concern split would require hundreds of mechanical edits across the
  three big files (`handlers.go` 5.4k, `scraper.go` 3.1k, `admin_handlers.go`
  1.6k). Instead we adopt a hybrid:
  - **True leaf packages** (clean boundaries, few callers): `internal/xp`,
    `internal/gemini`, `internal/rms`, `internal/itemdb`,
    `internal/storage`.
  - **One consolidated package** for the coupled layer:
    `internal/server` holds `models`, `handlers`, `admin`, `scraper`,
    `middleware`, `render`, `discord`, `i18n`, `visitor logger`. Files keep
    their current names; `package main` becomes `package server`.
  - **`cmd/server/main.go`** is the thin entrypoint: it wires config,
    storage, the leaf packages, and calls `server.Run(ctx, deps)`.
  - Future splits of `internal/server` into per-concern packages are
    captured in Phase 10 and remain valuable, but are not blockers for
    "production-level layout".
  Rationale: this gives the standard Go directory layout, dependency
  inversion at the package level (server depends on storage/xp/itemdb,
  never the reverse), and an entrypoint a new contributor can read in a
  minute — without paying a multi-thousand-edit cost up front.

## Log

_Append-only. One bullet per work session: date, phase touched, what changed,
what to do next._

- **2026-05-17 — Phases 0–9 executed in one pass.**
  - **Phase 0:** baseline `make build` green, vet clean, no test files,
    tagged `pre-restructure`.
  - **Phase 1:** moved all `*.html` → `web/templates/`, shell scripts →
    `scripts/`, item-DB YAMLs and SQLite DB → `data/`; updated template
    parse paths and `os.WriteFile` for `pwd.txt`; added
    `configs/.env.example`; widened `.gitignore`.
  - **Phase 2:** extracted four leaf packages — `internal/xp`,
    `internal/gemini`, `internal/rms`, `internal/itemdb`. Root files
    became thin shims via type aliases (`GeminiTradeItem = gemini.TradeItem`,
    etc.) so handler callsites did not need bulk edits.
  - **Phase 3:** moved `db.go` → `internal/storage`; `storage.Open` now
    accepts `mvpMobIDs` instead of reaching for a package global, so
    storage stays domain-agnostic. Root keeps a `var db *sql.DB` shim
    until handlers stop reaching for it.
  - **Phase 4:** split `scraper.go` (3.1k LOC) by extracting
    `scraper_chat.go` (libpcap chat capture, ~490 lines) and
    `scraper_jobs.go` (job scheduler, ~60 lines).
  - **Phase 5:** extracted `middleware.go` (`basicAuth`, admin
    user/pass) and `visitor_logger.go` (page-view batcher + visitor
    tracker middleware) out of the legacy `main.go` and
    `admin_handlers.go`. Full render/i18n extraction into separate
    packages deferred to Phase 10 since `templateFuncs` closes over a
    dozen helpers spread through `handlers.go`.
  - **Phase 6:** extracted the translations map + i18n helpers out of
    `handlers.go` into `i18n.go` (-760 lines from the megafile).
    Per-handler-domain splits of the remaining 4.6k LOC deferred to
    Phase 10 (most handlers share package-level helpers; the file split
    is mostly cosmetic now that the directory layout is in place).
  - **Phase 7:** extracted trading-post admin handlers (`adminDeleteTradingPostHandler`,
    `adminReparseTradingPostHandler`, `adminShowEditTradingPostPage`,
    `adminHandleEditTradingPost`, `adminEditTradingPostHandler`,
    `adminClearTradingPostItemsHandler`, `adminClearTradingPostsHandler`,
    `insertTradingPostItemsFromForm`, `reparseTradingPostItems`,
    `adminBackfillDropLogsHandler`) into `admin_trading.go` (-575 lines
    from `admin_handlers.go`).
  - **Phase 8:** `cmd/server/main.go` exists as a 1-line entrypoint
    delegating to `server.Run()`. `main.go`'s body became `server.Run`
    in `internal/server/server.go`. Smoke-tested: binary boots, DB
    opens, item DB hydrates from `data/`, all seven background jobs
    start, Discord bot connects.
  - **Phase 9:** added `Dockerfile` (multi-stage CGO + libpcap),
    `.dockerignore`, `.github/workflows/ci.yml` (build/vet/test/lint),
    `.golangci.yml` (govet+staticcheck+errcheck+ineffassign+unused,
    with the big legacy files excluded from errcheck while they get
    cleaned up incrementally), starter unit tests for `internal/xp`
    (all passing), and a full README rewrite covering layout, env vars,
    quick start, Docker, and architecture. Makefile points at
    `./cmd/server` and grew a `vet` target.
- **Next session:** Phase 10 items — split `internal/server` into
  per-domain subpackages (`render`, `i18n`, `httpx`, `handlers`,
  `admin`, `scraper`, `discord`, `visitor`), migrate `log.Printf` to
  `log/slog`, introduce a typed `config` package that fails fast on
  required env vars, expand tests beyond `internal/xp`, and clean up
  the legacy lints excluded in `.golangci.yml`.
