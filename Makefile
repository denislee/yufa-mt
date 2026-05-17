# Makefile for Go Project

# --- Variables ---

# Go command
GOCMD=go

# Build tags
BUILD_TAGS=-tags fts5

# Go build command
GOBUILD=$(GOCMD) build $(BUILD_TAGS)

# Go clean command
GOCLEAN=$(GOCMD) clean

# Go run command
GORUN=$(GOCMD) run $(BUILD_TAGS)

# Go mod tidy command
GOTIDY=$(GOCMD) mod tidy

# Go format command
GOFMT=$(GOCMD) fmt ./...

# Go test command
GOTEST=$(GOCMD) test $(BUILD_TAGS) ./...

# Name of the output binary
BINARY_NAME=yufa-mt

# Path to the main package
CMD_PATH=./cmd/server

# Port the application listens on
PORT=8080

# URL to open in the browser
APP_URL=http://localhost:$(PORT)

# Generated files to clean
GENERATED_FILES=data/pwd.txt


# --- Targets ---

# The .PHONY directive tells make that these targets are not files
.PHONY: all build run kill-port open-browser clean tidy fmt lint test vet help

# Default target: running 'make' will be the same as 'make all'
all: build

# Build the application binary
build:
	@echo "Building $(BINARY_NAME) with tags: '$(BUILD_TAGS)'..."
	$(GOBUILD) -o $(BINARY_NAME) $(CMD_PATH)
	@echo "$(BINARY_NAME) built successfully."

# Run go vet on all packages
vet:
	@echo "Running go vet with tags: '$(BUILD_TAGS)'..."
	$(GOCMD) vet $(BUILD_TAGS) ./...

# Kill any process listening on $(PORT)
kill-port:
	@PIDS=$$( (lsof -ti tcp:$(PORT) 2>/dev/null) || (fuser -n tcp $(PORT) 2>/dev/null) ); \
	if [ -n "$$PIDS" ]; then \
		echo "Killing process(es) on port $(PORT): $$PIDS"; \
		kill -9 $$PIDS 2>/dev/null || true; \
		sleep 1; \
	else \
		echo "No process listening on port $(PORT)."; \
	fi

# Open the default browser to $(APP_URL) once the server is up
open-browser:
	@( \
		for i in 1 2 3 4 5 6 7 8 9 10 15 20; do \
			if (echo > /dev/tcp/127.0.0.1/$(PORT)) >/dev/null 2>&1; then \
				break; \
			fi; \
			sleep 0.5; \
		done; \
		echo "Opening $(APP_URL) in default browser..."; \
		if command -v xdg-open >/dev/null 2>&1; then xdg-open "$(APP_URL)" >/dev/null 2>&1 & \
		elif command -v open >/dev/null 2>&1; then open "$(APP_URL)" >/dev/null 2>&1 & \
		elif command -v start >/dev/null 2>&1; then start "" "$(APP_URL)" >/dev/null 2>&1 & \
		else echo "No supported browser opener found. Visit $(APP_URL) manually."; \
		fi \
	) &

# Build, free the port, launch the browser, and run the application
run: build kill-port open-browser
	@echo "Starting $(BINARY_NAME) on port $(PORT)..."
	./$(BINARY_NAME)

# Clean the project: remove binary and other generated files
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f $(GENERATED_FILES)
	@echo "Clean complete."

# Tidy dependencies (updates go.mod and go.sum)
tidy:
	@echo "Tidying dependencies..."
	$(GOTIDY)

# Format all Go code in the project
fmt:
	@echo "Formatting code..."
	$(GOFMT)

# Run tests
test:
	@echo "Running tests with tags: '$(BUILD_TAGS)'..."
	$(GOTEST)

# Run linter (requires golangci-lint)
lint:
	@echo "Linting code..."
	@command -v golangci-lint >/dev/null 2>&1 || (echo "golangci-lint not found. Please install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)
	golangci-lint run --build-tags "$(BUILD_TAGS)"

# Display this help message
help:
	@echo "Available commands for $(BINARY_NAME):"
	@echo ""
	@echo "  all           (Default) Alias for build"
	@echo "  build         Build the application binary (output: $(BINARY_NAME))"
	@echo "  run           Build, free port $(PORT), launch browser, and run the app"
	@echo "  kill-port     Kill any process listening on port $(PORT)"
	@echo "  open-browser  Wait for port $(PORT), then open $(APP_URL) in default browser"
	@echo "  clean         Remove the binary and generated files ($(GENERATED_FILES))"
	@echo "  tidy          Tidy Go module dependencies (go.mod)"
	@echo "  fmt           Format all Go source code"
	@echo "  lint          Run the linter with build tags (requires golangci-lint)"
	@echo "  test          Run unit tests with build tags"
	@echo ""
