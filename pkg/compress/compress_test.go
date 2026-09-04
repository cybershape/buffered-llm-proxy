package compress

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestSelectEncoding(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		expected Encoding
	}{
		{"empty", "", EncodingNone},
		{"gzip only", "gzip", EncodingGzip},
		{"zstd only", "zstd", EncodingZstd},
		{"gzip and zstd", "gzip, zstd", EncodingZstd},
		{"zstd and gzip", "zstd, gzip", EncodingZstd},
		{"gzip preferred by q", "gzip;q=1.0, zstd;q=0.5", EncodingGzip},
		{"zstd preferred by q", "gzip;q=0.3, zstd;q=0.8", EncodingZstd},
		{"gzip zero q", "gzip;q=0, zstd", EncodingZstd},
		{"zstd zero q", "zstd;q=0, gzip", EncodingGzip},
		{"both zero q", "zstd;q=0, gzip;q=0", EncodingNone},
		{"wildcard", "*", EncodingZstd},
		{"wildcard with gzip zero", "*;q=1.0, gzip;q=0", EncodingZstd},
		{"wildcard with zstd zero", "*;q=1.0, zstd;q=0", EncodingGzip},
		{"other encoding", "deflate, br", EncodingNone},
		{"identity only", "identity", EncodingNone},
		{"complex header", "text/html, gzip;q=0.9, zstd;q=0.95, br;q=1.0", EncodingZstd},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := SelectEncoding(tc.header)
			if actual != tc.expected {
				t.Fatalf("for header %q, expected %s, got %s", tc.header, tc.expected, actual)
			}
		})
	}
}

func TestGzipCompression(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Accept-Encoding", "gzip")

	rw, cleanup := WrapResponseWriter(rec, req, nil)
	defer cleanup()

	msg := "hello world from gzip stream! "
	_, err := rw.Write([]byte(msg))
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	cleanup()

	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("expected Content-Encoding gzip, got %s", rec.Header().Get("Content-Encoding"))
	}
	if !strings.Contains(rec.Header().Get("Vary"), "Accept-Encoding") {
		t.Fatalf("expected Vary Accept-Encoding, got %s", rec.Header().Get("Vary"))
	}

	gzReader, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}
	if string(decompressed) != msg {
		t.Fatalf("expected %q, got %q", msg, string(decompressed))
	}
}

func TestZstdCompression(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Accept-Encoding", "zstd")

	rw, cleanup := WrapResponseWriter(rec, req, nil)
	defer cleanup()

	msg := "hello world from zstd stream! Testing repeating token content 1234567890."
	_, err := rw.Write([]byte(msg))
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	cleanup()

	if rec.Header().Get("Content-Encoding") != "zstd" {
		t.Fatalf("expected Content-Encoding zstd, got %s", rec.Header().Get("Content-Encoding"))
	}
	if !strings.Contains(rec.Header().Get("Vary"), "Accept-Encoding") {
		t.Fatalf("expected Vary Accept-Encoding, got %s", rec.Header().Get("Vary"))
	}

	zstdReader, err := zstd.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("failed to create zstd reader: %v", err)
	}
	defer zstdReader.Close()

	decompressed, err := io.ReadAll(zstdReader)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}
	if string(decompressed) != msg {
		t.Fatalf("expected %q, got %q", msg, string(decompressed))
	}
}

func TestStreamingFlushZstd(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	req.Header.Set("Accept-Encoding", "zstd")

	rw, cleanup := WrapResponseWriter(rec, req, nil)
	defer cleanup()

	part1 := "data: {\"delta\":\"part1\"}\n\n"
	_, _ = rw.Write([]byte(part1))
	rw.Flush()

	lenAfterPart1 := rec.Body.Len()
	if lenAfterPart1 == 0 {
		t.Fatalf("expected immediate flushed bytes after part1 flush, got 0")
	}

	part2 := "data: {\"delta\":\"part2\"}\n\n"
	_, _ = rw.Write([]byte(part2))
	rw.Flush()

	lenAfterPart2 := rec.Body.Len()
	if lenAfterPart2 <= lenAfterPart1 {
		t.Fatalf("expected more flushed bytes after part2 flush, was %d now %d", lenAfterPart1, lenAfterPart2)
	}

	cleanup()

	zstdReader, err := zstd.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("failed to init zstd reader: %v", err)
	}
	defer zstdReader.Close()

	all, err := io.ReadAll(zstdReader)
	if err != nil {
		t.Fatalf("failed to read decompressed stream: %v", err)
	}
	if string(all) != part1+part2 {
		t.Fatalf("expected %q, got %q", part1+part2, string(all))
	}
}

func TestStreamingFlushGzip(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	req.Header.Set("Accept-Encoding", "gzip")

	rw, cleanup := WrapResponseWriter(rec, req, nil)
	defer cleanup()

	part1 := "data: {\"delta\":\"part1\"}\n\n"
	_, _ = rw.Write([]byte(part1))
	rw.Flush()

	lenAfterPart1 := rec.Body.Len()
	if lenAfterPart1 == 0 {
		t.Fatalf("expected immediate flushed bytes after part1 flush, got 0")
	}

	part2 := "data: {\"delta\":\"part2\"}\n\n"
	_, _ = rw.Write([]byte(part2))
	rw.Flush()

	cleanup()

	gzReader, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("failed to init gzip reader: %v", err)
	}
	defer gzReader.Close()

	all, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("failed to read decompressed stream: %v", err)
	}
	if string(all) != part1+part2 {
		t.Fatalf("expected %q, got %q", part1+part2, string(all))
	}
}

func TestSkipCompressionAlreadyEncoded(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/already-encoded", nil)
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	rw, cleanup := WrapResponseWriter(rec, req, nil)
	defer cleanup()

	rw.Header().Set("Content-Encoding", "custom-encoding")
	rw.WriteHeader(http.StatusOK)
	rawPayload := "raw bytes not compressed by proxy"
	_, _ = rw.Write([]byte(rawPayload))
	cleanup()

	if rec.Header().Get("Content-Encoding") != "custom-encoding" {
		t.Fatalf("expected Content-Encoding custom-encoding, got %s", rec.Header().Get("Content-Encoding"))
	}
	if rec.Body.String() != rawPayload {
		t.Fatalf("expected verbatim raw body, got %s", rec.Body.String())
	}
}

func TestSkipCompressionNoContent(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/no-content", nil)
	req.Header.Set("Accept-Encoding", "zstd, gzip")

	rw, cleanup := WrapResponseWriter(rec, req, nil)
	defer cleanup()

	rw.WriteHeader(http.StatusNoContent)
	cleanup()

	if rec.Header().Get("Content-Encoding") != "" {
		t.Fatalf("expected no Content-Encoding for 204, got %s", rec.Header().Get("Content-Encoding"))
	}
}

type testMetricsRecorder struct {
	uncompressed int64
	compressed   int64
}

func (m *testMetricsRecorder) AddCompressionBytes(uncompressed, compressed int64) {
	m.uncompressed += uncompressed
	m.compressed += compressed
}

func TestCompressionMetricsRecorder(t *testing.T) {
	mr := &testMetricsRecorder{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Accept-Encoding", "gzip")

	rw, cleanup := WrapResponseWriter(rec, req, mr)
	msg := strings.Repeat("Hello world from metrics test! ", 20)
	_, _ = rw.Write([]byte(msg))
	cleanup()

	if mr.uncompressed != int64(len(msg)) {
		t.Fatalf("expected uncompressed %d, got %d", len(msg), mr.uncompressed)
	}
	if mr.compressed == 0 || mr.compressed >= mr.uncompressed {
		t.Fatalf("expected compressed bytes < uncompressed bytes, got %d vs %d", mr.compressed, mr.uncompressed)
	}
}
