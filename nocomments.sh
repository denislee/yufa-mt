#!/bin/bash

# This script creates and runs a small Go program to safely remove comments
# from all .go files in the current directory and subdirectories.
# This approach uses Go's own parser, so it correctly handles
# strings, runes, and other language constructs without breaking your code.

# 1. Check if the Go compiler is available
if ! command -v go &> /dev/null; then
    echo "Error: The Go compiler ('go') was not found."
    echo "Please install Go to use this script."
    exit 1
fi

# 2. Define the Go program as a string variable
GO_PROGRAM=$(cat <<'EOF'
package main

import (
	"fmt"
	"go/parser"
	"go/printer"
	"go/token"
	"io/ioutil"
	"os"
	"path/filepath"
)

func main() {
	// Walk the current directory
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Check if it's a Go file
		if !info.IsDir() && filepath.Ext(path) == ".go" {
			// Skip the temporary file this script creates
			if filepath.Base(path) == "tmp_remove_comments.go" {
				return nil
			}

			fmt.Printf("Processing %s...\n", path)
			if err := removeComments(path); err != nil {
				fmt.Fprintf(os.Stderr, "Error processing %s: %v\n", path, err)
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error walking directory: %v\n", err)
		os.Exit(1)
	}
}

func removeComments(filename string) error {
	fset := token.NewFileSet()

	// Read the file content
	src, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	// Parse the file. By passing the mode '0', we tell the parser
	// *not* to parse comments and attach them to the AST.
	f, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		return err
	}

	// Open the same file for writing (truncating it)
	out, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer out.Close()

	// Print the AST (the code structure) back to the file.
	// Since the comments were never parsed, they are not printed.
	// This also formats the code, similar to 'gofmt'.
	cfg := printer.Config{Mode: printer.TabIndent | printer.UseSpaces, Tabwidth: 8}
	return cfg.Fprint(out, fset, f)
}
EOF
)

# 3. Create a temporary Go file
TMP_GO_FILE="tmp_remove_comments.go"
echo "$GO_PROGRAM" > "$TMP_GO_FILE"

# 4. Run the Go program
echo "Starting comment removal..."
go run "$TMP_GO_FILE"

# 5. Clean up the temporary file
rm "$TMP_GO_FILE"

echo "Comment removal complete."
