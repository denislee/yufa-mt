// Package web embeds the application's HTML templates and static
// assets so the binary is self-contained and does not depend on a
// relative filesystem layout at runtime.
package web

import "embed"

//go:embed templates/*.html
var Templates embed.FS

//go:embed static
var Static embed.FS
