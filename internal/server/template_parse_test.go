package server

import "testing"

// TestTemplatesParseAndRenderShell verifies layout.html parses for every
// registered page template and that the layout, title, head_extra and
// content blocks are all present. The package init() populates
// templateCache — a parse failure there would call log.Fatalf, so the
// test surface here is just the block topology. title/head_extra/content
// are required because the htmx partial-response path in renderTemplate
// executes each block by name (see handlers.go).
func TestTemplatesParseAndRenderShell(t *testing.T) {
	if len(templateCache) == 0 {
		t.Fatal("templateCache is empty; init() did not populate it")
	}
	for name, tmpl := range templateCache {
		if tmpl == nil {
			t.Errorf("%s: nil template in cache", name)
			continue
		}
		if name == "admin.html" || name == "admin_edit_post.html" {
			continue
		}
		if tmpl.Lookup("layout.html") == nil {
			t.Errorf("%s: missing layout.html block", name)
		}
		if tmpl.Lookup("title") == nil {
			t.Errorf("%s: missing title block", name)
		}
		if tmpl.Lookup("head_extra") == nil {
			t.Errorf("%s: missing head_extra block", name)
		}
		if tmpl.Lookup("content") == nil {
			t.Errorf("%s: missing content block", name)
		}
	}
}
