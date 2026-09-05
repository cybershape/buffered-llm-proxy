package aggregator

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"buffered-proxy/pkg/metrics"
	"buffered-proxy/pkg/semantic"
	"buffered-proxy/pkg/sse"
)

type StreamPipeline struct {
	cfg        BufferConfig
	metrics    *metrics.StreamMetrics
	parser     *semantic.Parser
	serializer *semantic.Serializer
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

func (p *StreamPipeline) ProcessStream(ctx context.Context, upstream io.ReadCloser, downstream io.Writer) error {
	defer upstream.Close()

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
				case *semantic.ContentSegment:
					p.metrics.IncContentFragmentsIn()
				case *semantic.ToolCallSegment:
					for range v.Calls {
						p.metrics.IncToolArgumentFragmentsIn()
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
