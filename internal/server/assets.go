package server

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"html/template"
	"io/fs"
	"log"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/denislee/yufa-mt/web"
)

// staticAsset holds an embedded file plus its pre-computed gzip
// encoding, so requests for /static/* never re-gzip on the hot path.
type staticAsset struct {
	contentType string
	raw         []byte
	gzipped     []byte
}

var staticAssets = make(map[string]*staticAsset)

// staticAssetHashes maps "/static/<path>" to a short content hash used as
// a cache-busting query string. Populated once on startup by
// initStaticAssetHashes; read by the "asset" template function and by
// cacheStatic to decide whether to serve a response as immutable.
var (
	staticAssetHashes     map[string]string
	staticAssetHashesOnce sync.Once
)

func initStaticAssetHashes() {
	staticAssetHashesOnce.Do(func() {
		staticAssetHashes = make(map[string]string)
		err := fs.WalkDir(web.Static, "static", func(p string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			b, err := fs.ReadFile(web.Static, p)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(b)
			h := hex.EncodeToString(sum[:])[:10]
			urlPath := "/" + p // "static/app.css" -> "/static/app.css"
			staticAssetHashes[urlPath] = h

			// Resolve a Content-Type up front from the file extension.
			// Fallback to a text default when mime.TypeByExtension is
			// silent (rare for our handful of asset types).
			ct := mime.TypeByExtension(path.Ext(p))
			if ct == "" {
				ct = "application/octet-stream"
			}

			// Pre-compress once on startup. For asset types that don't
			// benefit (already-compressed binaries) we'd skip gzip, but
			// every file we currently embed is text and compresses well.
			var gzBuf bytes.Buffer
			gw, _ := gzip.NewWriterLevel(&gzBuf, gzip.BestCompression)
			if _, werr := gw.Write(b); werr != nil {
				log.Printf("[W] [HTTP] gzip %s: %v", p, werr)
			}
			_ = gw.Close()

			staticAssets[urlPath] = &staticAsset{
				contentType: ct,
				raw:         b,
				gzipped:     gzBuf.Bytes(),
			}
			return nil
		})
		if err != nil {
			log.Printf("[W] [HTTP] could not hash static assets: %v", err)
		}
	})
}

// serveStaticAsset handles GET requests for /static/* using pre-loaded
// and pre-compressed embedded bytes. Bypasses http.FileServer so we
// don't pay gzip-per-request and can serve immutable cache headers when
// the request URL carries a matching content hash.
func serveStaticAsset(w http.ResponseWriter, r *http.Request) {
	a, ok := staticAssets[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if hasAssetHash(r.URL.Path, r.URL.RawQuery) {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=86400")
	}
	w.Header().Set("Content-Type", a.contentType)

	useGzip := strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") && len(a.gzipped) > 0 && len(a.gzipped) < len(a.raw)
	body := a.raw
	if useGzip {
		body = a.gzipped
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(body)
}

// assetURL returns the public URL for a static asset with a content-hash
// query string appended, e.g. "/static/app.css" -> "/static/app.css?v=ab12cd34ef".
// Pairs with cacheStatic, which serves any hashed request as immutable.
func assetURL(path string) template.HTMLAttr {
	if h, ok := staticAssetHashes[path]; ok {
		return template.HTMLAttr(path + "?v=" + h)
	}
	return template.HTMLAttr(path)
}

// earlyHintLinks returns RFC 8288 Link header values for the critical
// site assets, ready to be passed to middleware.EarlyHints. Uses the
// hashed URL so cache-busts are honored.
func earlyHintLinks() []string {
	initStaticAssetHashes()
	mk := func(p, as string) string {
		return "<" + string(assetURL(p)) + ">; rel=preload; as=" + as
	}
	return []string{
		mk("/static/tailwind.css", "style"),
		mk("/static/app.css", "style"),
		mk("/static/htmx.min.js", "script"),
		mk("/static/alpine.min.js", "script"),
		mk("/static/app.js", "script"),
	}
}

// hasAssetHash reports whether a request URL carries a "v=" query string
// matching the known hash for that path. Used to decide cache-control.
func hasAssetHash(urlPath, query string) bool {
	if query == "" {
		return false
	}
	h, ok := staticAssetHashes[urlPath]
	if !ok {
		return false
	}
	for _, part := range strings.Split(query, "&") {
		if part == "v="+h {
			return true
		}
	}
	return false
}
