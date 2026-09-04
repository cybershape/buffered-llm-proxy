package compress

import (
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

type MetricsRecorder interface {
	AddCompressionBytes(uncompressed, compressed int64)
}

type byteCounter struct {
	http.ResponseWriter
	written int64
}

func (bc *byteCounter) Write(p []byte) (int, error) {
	n, err := bc.ResponseWriter.Write(p)
	atomic.AddInt64(&bc.written, int64(n))
	return n, err
}

type Encoding string

const (
	EncodingNone Encoding = ""
	EncodingZstd Encoding = "zstd"
	EncodingGzip Encoding = "gzip"
)

var gzipPool = sync.Pool{
	New: func() interface{} {
		gw, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		return gw
	},
}

var zstdPool = sync.Pool{
	New: func() interface{} {
		enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		return enc
	},
}

func SelectEncoding(acceptHeader string) Encoding {
	if acceptHeader == "" {
		return EncodingNone
	}

	type item struct {
		q       float64
		present bool
	}

	var (
		zstdItem item
		gzipItem item
		starItem item
	)

	parts := strings.Split(acceptHeader, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		encoding := part
		q := 1.0
		if semi := strings.IndexByte(part, ';'); semi != -1 {
			encoding = strings.TrimSpace(part[:semi])
			params := part[semi+1:]
			for _, param := range strings.Split(params, ";") {
				param = strings.TrimSpace(param)
				if strings.HasPrefix(strings.ToLower(param), "q=") {
					valStr := strings.TrimSpace(param[2:])
					if val, err := strconv.ParseFloat(valStr, 64); err == nil {
						if val < 0 {
							val = 0
						} else if val > 1 {
							val = 1
						}
						q = val
					}
				}
			}
		}

		encoding = strings.ToLower(encoding)
		switch encoding {
		case "zstd":
			zstdItem = item{q: q, present: true}
		case "gzip":
			gzipItem = item{q: q, present: true}
		case "*":
			starItem = item{q: q, present: true}
		}
	}

	if !zstdItem.present && starItem.present {
		zstdItem = item{q: starItem.q, present: true}
	}
	if !gzipItem.present && starItem.present {
		gzipItem = item{q: starItem.q, present: true}
	}

	if zstdItem.present && zstdItem.q > 0 && gzipItem.present && gzipItem.q > 0 {
		if zstdItem.q >= gzipItem.q {
			return EncodingZstd
		}
		return EncodingGzip
	}
	if zstdItem.present && zstdItem.q > 0 {
		return EncodingZstd
	}
	if gzipItem.present && gzipItem.q > 0 {
		return EncodingGzip
	}

	return EncodingNone
}

type ResponseWriter struct {
	http.ResponseWriter
	encoding          Encoding
	wroteHeader       bool
	statusCode        int
	gzipWriter        *gzip.Writer
	zstdEncoder       *zstd.Encoder
	counter           *byteCounter
	uncompressedBytes int64
	metrics           MetricsRecorder
	recordedMetrics   bool
	closed            bool
	mu                sync.Mutex
}

func WrapResponseWriter(w http.ResponseWriter, r *http.Request, mr MetricsRecorder) (*ResponseWriter, func()) {
	enc := SelectEncoding(r.Header.Get("Accept-Encoding"))
	if r.Header.Get("Range") != "" {
		enc = EncodingNone
	}

	rw := &ResponseWriter{
		ResponseWriter: w,
		encoding:       enc,
		counter:        &byteCounter{ResponseWriter: w},
		metrics:        mr,
	}

	cleanup := func() {
		_ = rw.Close()
	}

	return rw, cleanup
}

func (rw *ResponseWriter) SelectedEncoding() Encoding {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.encoding
}

func (rw *ResponseWriter) WriteHeader(statusCode int) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.wroteHeader {
		return
	}
	rw.wroteHeader = true
	rw.statusCode = statusCode

	if rw.encoding == EncodingNone ||
		statusCode == http.StatusNoContent ||
		statusCode == http.StatusNotModified ||
		(statusCode >= 100 && statusCode < 200) ||
		rw.Header().Get("Content-Encoding") != "" {
		rw.encoding = EncodingNone
		rw.ResponseWriter.WriteHeader(statusCode)
		return
	}

	rw.Header().Del("Content-Length")
	rw.Header().Set("Content-Encoding", string(rw.encoding))
	rw.Header().Add("Vary", "Accept-Encoding")

	switch rw.encoding {
	case EncodingZstd:
		enc := zstdPool.Get().(*zstd.Encoder)
		enc.Reset(rw.counter)
		rw.zstdEncoder = enc
	case EncodingGzip:
		gw := gzipPool.Get().(*gzip.Writer)
		gw.Reset(rw.counter)
		rw.gzipWriter = gw
	}

	rw.ResponseWriter.WriteHeader(statusCode)
}

func (rw *ResponseWriter) Write(p []byte) (int, error) {
	rw.mu.Lock()
	if !rw.wroteHeader {
		rw.mu.Unlock()
		rw.WriteHeader(http.StatusOK)
		rw.mu.Lock()
	}
	defer rw.mu.Unlock()

	switch rw.encoding {
	case EncodingZstd:
		atomic.AddInt64(&rw.uncompressedBytes, int64(len(p)))
		return rw.zstdEncoder.Write(p)
	case EncodingGzip:
		atomic.AddInt64(&rw.uncompressedBytes, int64(len(p)))
		return rw.gzipWriter.Write(p)
	default:
		return rw.ResponseWriter.Write(p)
	}
}

func (rw *ResponseWriter) Flush() {
	rw.mu.Lock()
	if !rw.wroteHeader {
		rw.mu.Unlock()
		rw.WriteHeader(http.StatusOK)
		rw.mu.Lock()
	}

	switch rw.encoding {
	case EncodingZstd:
		if rw.zstdEncoder != nil {
			_ = rw.zstdEncoder.Flush()
		}
	case EncodingGzip:
		if rw.gzipWriter != nil {
			_ = rw.gzipWriter.Flush()
		}
	}
	rw.mu.Unlock()

	if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (rw *ResponseWriter) Close() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.closed {
		return nil
	}
	rw.closed = true

	if !rw.wroteHeader {
		return nil
	}

	var err error
	switch rw.encoding {
	case EncodingZstd:
		if rw.zstdEncoder != nil {
			err = rw.zstdEncoder.Close()
			zstdPool.Put(rw.zstdEncoder)
			rw.zstdEncoder = nil
		}
	case EncodingGzip:
		if rw.gzipWriter != nil {
			err = rw.gzipWriter.Close()
			gzipPool.Put(rw.gzipWriter)
			rw.gzipWriter = nil
		}
	}

	if rw.metrics != nil && !rw.recordedMetrics && rw.encoding != EncodingNone {
		rw.recordedMetrics = true
		uncompressed := atomic.LoadInt64(&rw.uncompressedBytes)
		compressed := atomic.LoadInt64(&rw.counter.written)
		rw.metrics.AddCompressionBytes(uncompressed, compressed)
	}

	if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}

	return err
}

func (rw *ResponseWriter) CloseNotify() <-chan bool {
	if cn, ok := rw.ResponseWriter.(http.CloseNotifier); ok {
		return cn.CloseNotify()
	}
	return nil
}

func (rw *ResponseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}
