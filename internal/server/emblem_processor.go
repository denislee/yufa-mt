package server

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"image"
	"image/color"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Ragnarok Online guild emblems use magenta (#FF00FF) as the chroma-key
// background. Allow a small tolerance for color quantization / JPEG bleed.
const (
	emblemKeyRMin = 240
	emblemKeyGMax = 20
	emblemKeyBMin = 240
)

// emblemHTTPClient is the shared client used by all concurrent emblem
// downloads. Reusing a single client lets the transport pool keep
// connections to the emblem host alive across the (large) backfill jobs.
var emblemHTTPClient = func() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 50
	transport.MaxIdleConnsPerHost = 10
	transport.MaxConnsPerHost = 10
	transport.IdleConnTimeout = 90 * time.Second
	return &http.Client{Timeout: 20 * time.Second, Transport: transport}
}()

// emblemDir returns the on-disk directory where processed emblems are
// stored. It lives next to the SQLite DB so it shares the same backup /
// runtime data path. Returns "" if the config is not yet loaded.
func emblemDir() string {
	if appConfig == nil || appConfig.DBPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(appConfig.DBPath), "emblems")
}

// emblemFilename derives a stable filename for a given emblem URL so that
// re-running the processor is a no-op when the URL hasn't changed.
func emblemFilename(url string) string {
	sum := sha1.Sum([]byte(url))
	return hex.EncodeToString(sum[:]) + ".png"
}

// processGuildEmblems downloads each guild's emblem (when a URL is set),
// keys out the magenta background to transparent, and stores the result
// under emblemDir() as PNG. The DB column emblem_local_path is updated
// to point at the served path (/emblems/<hash>.png).
func processGuildEmblems() {
	dir := emblemDir()
	if dir == "" {
		log.Println("[W] [Emblem] Skipping emblem processing: emblem directory not configured.")
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[E] [Emblem] Failed to create emblem dir %s: %v", dir, err)
		return
	}

	rows, err := srv.db.Query(`SELECT name, COALESCE(emblem_url, ''), COALESCE(emblem_local_path, '')
		FROM guilds WHERE is_active = 1`)
	if err != nil {
		log.Printf("[E] [Emblem] Failed to query guilds: %v", err)
		return
	}
	type job struct{ name, url, local string }
	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.name, &j.url, &j.local); err != nil {
			log.Printf("[W] [Emblem] Failed to scan guild row: %v", err)
			continue
		}
		jobs = append(jobs, j)
	}
	rows.Close()

	processed, skipped, failed := 0, 0, 0
	for _, j := range jobs {
		if j.url == "" {
			continue
		}
		want := "/emblems/" + emblemFilename(j.url)
		outPath := filepath.Join(dir, emblemFilename(j.url))

		// If the on-disk file already exists and the DB already points
		// at the same hash, this URL was processed previously.
		if j.local == want {
			if _, statErr := os.Stat(outPath); statErr == nil {
				skipped++
				continue
			}
		}

		if err := downloadAndKeyEmblem(j.url, outPath); err != nil {
			log.Printf("[W] [Emblem] %s: %v", j.name, err)
			failed++
			continue
		}
		if _, err := srv.db.Exec(`UPDATE guilds SET emblem_local_path = ? WHERE name = ?`, want, j.name); err != nil {
			log.Printf("[W] [Emblem] %s: failed to update DB: %v", j.name, err)
			failed++
			continue
		}
		processed++
	}
	log.Printf("[I] [Emblem] Emblem processing complete: %d processed, %d skipped, %d failed.", processed, skipped, failed)
}

// downloadAndKeyEmblem fetches a URL, keys magenta to transparent, and
// writes the result atomically as PNG to outPath.
func downloadAndKeyEmblem(url, outPath string) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", scraperClient.UserAgent)

	resp, err := emblemHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download: status %d", resp.StatusCode)
	}

	// Cap read size so a malicious URL can't exhaust memory.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	img, _, err := image.Decode(bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("decode image: %w", err)
	}

	keyed := keyMagentaToTransparent(img)

	tmp, err := os.CreateTemp(filepath.Dir(outPath), ".emblem-*.png")
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	tmpName := tmp.Name()
	if err := png.Encode(tmp, keyed); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("encode png: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close tmp: %w", err)
	}
	if err := os.Rename(tmpName, outPath); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// keyMagentaToTransparent walks every pixel and turns near-magenta into
// fully transparent. Non-magenta pixels are copied as fully opaque RGBA.
func keyMagentaToTransparent(src image.Image) *image.NRGBA {
	b := src.Bounds()
	out := image.NewNRGBA(b)
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			r16, g16, b16, a16 := src.At(x, y).RGBA()
			r := uint8(r16 >> 8)
			g := uint8(g16 >> 8)
			bl := uint8(b16 >> 8)
			a := uint8(a16 >> 8)
			if r >= emblemKeyRMin && g <= emblemKeyGMax && bl >= emblemKeyBMin {
				out.SetNRGBA(x, y, color.NRGBA{0, 0, 0, 0})
				continue
			}
			out.SetNRGBA(x, y, color.NRGBA{r, g, bl, a})
		}
	}
	return out
}
