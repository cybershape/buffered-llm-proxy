package aggregator

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"buffered-proxy/pkg/metrics"
	"buffered-proxy/pkg/semantic"
	"buffered-proxy/pkg/sse"
)

type StreamPipeline struct {
	cfg          BufferConfig
	metrics      *metrics.StreamMetrics
	parser       *semantic.Parser
	serializer   *semantic.Serializer
	initialModel string
	startTime    time.Time
}

func NewStreamPipeline(cfg BufferConfig, m *metrics.StreamMetrics) *StreamPipeline {
	if m == nil {
		m = &metrics.StreamMetrics{}
	}
	return &StreamPipeline{
		cfg:        cfg,
		metrics:    m,
		parser:     semantic.NewParser(),
		serializer: semantic.NewSerializer(),
	}
}

func (p *StreamPipeline) SetRequestInfo(model string, startTime time.Time) {
	p.initialModel = model
	p.startTime = startTime
}

func extractCompletionTokens(usage interface{}) int64 {
	if m, ok := usage.(map[string]interface{}); ok {
		if ct, ok := m["completion_tokens"]; ok {
			switch v := ct.(type) {
			case float64:
				return int64(v)
			case int64:
				return v
			case int:
				return int64(v)
			}
		}
	}
	return 0
}

func (p *StreamPipeline) ProcessStream(ctx context.Context, upstream io.ReadCloser, downstream io.Writer) error {
	defer upstream.Close()

	reqStart := p.startTime
	if reqStart.IsZero() {
		reqStart = time.Now()
	}

	var (
		firstTokenMu   sync.Mutex
		firstTokenTime time.Time
		totalTokens    int64
		usageTokens    int64
	)

	recordFirstToken := func() {
		firstTokenMu.Lock()
		if firstTokenTime.IsZero() {
			firstTokenTime = time.Now()
		}
		firstTokenMu.Unlock()
	}

	defer func() {
		model := p.initialModel
		if model == "" {
			model = p.parser.Model()
		}
		if model == "" {
			model = "unknown"
		}

		uTokens := atomic.LoadInt64(&usageTokens)
		finalTokens := atomic.LoadInt64(&totalTokens)
		if uTokens > 0 {
			finalTokens = uTokens
		}

		firstTokenMu.Lock()
		ft := firstTokenTime
		firstTokenMu.Unlock()

		if !ft.IsZero() {
			ttft := ft.Sub(reqStart)
			genDuration := time.Since(ft)
			p.metrics.RecordModelMetrics(model, finalTokens, ttft, genDuration)
		} else if finalTokens > 0 {
			ttft := time.Since(reqStart)
			p.metrics.RecordModelMetrics(model, finalTokens, ttft, 0)
		}
	}()

	flusher, _ := downstream.(http.Flusher)
	buf := NewPendingBuffer(p.cfg, p.metrics)

	readerCtx, cancelReader := context.WithCancel(ctx)
	defer func() {
		cancelReader()
		buf.Close(readerCtx.Err())
	}()

	stopMonitor := make(chan struct{})
	defer close(stopMonitor)
	go func() {
		select {
		case <-readerCtx.Done():
			buf.Close(readerCtx.Err())
		case <-stopMonitor:
		}
	}()

	readerErrCh := make(chan error, 1)

	go func() {
		defer close(readerErrCh)
		sseReader := sse.NewReader(upstream)
		for {
			if readerCtx.Err() != nil {
				buf.Close(readerCtx.Err())
				return
			}

			ev, err := sseReader.ReadEvent()
			if err != nil {
				if errors.Is(err, io.EOF) {
					buf.Close(nil)
				} else {
					buf.Close(err)
				}
				readerErrCh <- err
				return
			}

			p.metrics.IncUpstreamEvents()
			p.metrics.AddUpstreamBytes(int64(len(ev.Data)))

			segs, parseErr := p.parser.ParseEvent(ev)
			if parseErr != nil {
				continue
			}

			for _, seg := range segs {
				switch v := seg.(type) {
				case *semantic.ReasoningSegment:
					p.metrics.IncReasoningFragmentsIn()
					recordFirstToken()
					atomic.AddInt64(&totalTokens, 1)
				case *semantic.ContentSegment:
					p.metrics.IncContentFragmentsIn()
					recordFirstToken()
					atomic.AddInt64(&totalTokens, 1)
				case *semantic.ToolCallSegment:
					for range v.Calls {
						p.metrics.IncToolArgumentFragmentsIn()
						recordFirstToken()
						atomic.AddInt64(&totalTokens, 1)
					}
				case *semantic.UsageSegment:
					if ct := extractCompletionTokens(v.Usage); ct > 0 {
						atomic.StoreInt64(&usageTokens, ct)
					}
				}

				if appendErr := buf.Append(readerCtx, seg); appendErr != nil {
					return
				}
			}
		}
	}()

	for {
		snapshot, closed, err := buf.Swap(ctx)
		if err != nil && !errors.Is(err, io.EOF) && len(snapshot) == 0 {
			cancelReader()
			return err
		}

		if len(snapshot) > 0 {
			flushed := false
			for _, seg := range snapshot {
				out := p.serializer.SerializeSegment(seg)
				if len(out) == 0 {
					continue
				}

				startWrite := time.Now()
				if _, writeErr := downstream.Write(out); writeErr != nil {
					cancelReader()
					return writeErr
				}
				p.metrics.AddDownstreamWrite(time.Since(startWrite))

				p.metrics.IncDownstreamEvents()
				p.metrics.AddDownstreamBytes(int64(len(out)))
				flushed = true

				switch seg.(type) {
				case *semantic.ReasoningSegment:
					p.metrics.IncReasoningEventsOut()
				case *semantic.ContentSegment:
					p.metrics.IncContentEventsOut()
				case *semantic.ToolCallSegment:
					p.metrics.IncToolEventsOut()
				}
			}
			if flushed && flusher != nil {
				flusher.Flush()
			}
		}

		if closed && buf.Len() == 0 {
			break
		}
	}

	return nil
}
