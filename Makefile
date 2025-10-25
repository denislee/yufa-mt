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
BINARY_NAME=app

# Generated files to clean
GENERATED_FILES=pwd.txt


# --- Targets ---

# The .PHONY directive tells make that these targets are not files
.PHONY: all build run clean tidy fmt lint test help

# Default target: running 'make' will be the same as 'make all'
all: build

# Build the application binary
build:
	@echo "Building $(BINARY_NAME) with tags: '$(BUILD_TAGS)'..."
	$(GOBUILD) -o $(BINARY_NAME) .
	@echo "$(BINARY_NAME) built successfully."

# Run the application
run:
	@echo "Running $(BINARY_NAME) with tags: '$(BUILD_TAGS)'..."
	$(GORUN) .

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
	@echo "  run           Run the application (compiles in memory)"
	@echo "  clean         Remove the binary and generated files ($(GENERATED_FILES))"
	@echo "  tidy          Tidy Go module dependencies (go.mod)"
	@echo "  fmt           Format all Go source code"
	@echo "  lint          Run the linter with build tags (requires golangci-lint)"
	@echo "  test          Run unit tests with build tags"
	@echo ""
