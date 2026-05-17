# syntax=docker/dockerfile:1.6
#
# Multi-stage build for Yufa-MT.
#
# CGO is required by mattn/go-sqlite3 and by gopacket's libpcap binding,
# so the runtime image is Debian slim (not distroless) — it ships glibc
# and libpcap.

FROM golang:1.24-bookworm AS builder

WORKDIR /src

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc \
      libc6-dev \
      libpcap-dev \
 && rm -rf /var/lib/apt/lists/*

# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=linux \
    go build -tags fts5 -trimpath -ldflags="-s -w" \
        -o /out/yufa-mt ./cmd/server

# ---- runtime stage ----
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      libpcap0.8 \
      tzdata \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 10001 yufa

WORKDIR /app
COPY --from=builder /out/yufa-mt /usr/local/bin/yufa-mt
COPY web/templates ./web/templates
COPY data/item_db_equip.yml data/item_db_etc.yml data/item_db_usable.yml ./data/

USER yufa
EXPOSE 8080

# market_data.db, pwd.txt, and chat capture output live under /app/data
# and should be mounted from a volume in production.
VOLUME ["/app/data"]

ENTRYPOINT ["/usr/local/bin/yufa-mt"]
