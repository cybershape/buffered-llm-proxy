package sse

import (
	"bytes"
	"io"
	"testing"
)

type chunkReader struct {
	data      []byte
	chunkSize int
	pos       int
}

func (c *chunkReader) Read(p []byte) (n int, err error) {
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	remaining := len(c.data) - c.pos
	toRead := c.chunkSize
	if toRead > remaining {
		toRead = remaining
	}
	if toRead > len(p) {
		toRead = len(p)
	}
	copy(p, c.data[c.pos:c.pos+toRead])
	c.pos += toRead
	return toRead, nil
}

func TestReaderSingleEvent(t *testing.T) {
	input := "data: {\"hello\":\"world\"}\n\n"
	r := NewReader(bytes.NewReader([]byte(input)))

	ev, err := r.ReadEvent()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(ev.Data) != "{\"hello\":\"world\"}" {
		t.Fatalf("unexpected data: %s", string(ev.Data))
	}

	_, err = r.ReadEvent()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

func TestReaderMultipleEventsAndComments(t *testing.T) {
	input := ":comment line\n" +
		"event: update\n" +
		"id: 123\n" +
		"data: line1\n" +
		"data: line2\n\n" +
		":another comment\n" +
		"data: [DONE]\n\n"

	r := NewReader(bytes.NewReader([]byte(input)))

	ev1, err := r.ReadEvent()
	if err != nil {
		t.Fatalf("unexpected error on ev1: %v", err)
	}
	if ev1.Type != "update" || ev1.ID != "123" || string(ev1.Data) != "line1\nline2" {
		t.Fatalf("unexpected ev1: %+v", ev1)
	}

	ev2, err := r.ReadEvent()
	if err != nil {
		t.Fatalf("unexpected error on ev2: %v", err)
	}
	if string(ev2.Data) != "[DONE]" {
		t.Fatalf("unexpected ev2 data: %s", string(ev2.Data))
	}

	_, err = r.ReadEvent()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

func TestReaderChunkedDelivery(t *testing.T) {
	input := "data: {\"key\":\"value\"}\n\n"
	cr := &chunkReader{data: []byte(input), chunkSize: 2}
	r := NewReader(cr)

	ev, err := r.ReadEvent()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(ev.Data) != "{\"key\":\"value\"}" {
		t.Fatalf("unexpected data: %s", string(ev.Data))
	}
}

func TestReaderEOFWithoutTrailingNewline(t *testing.T) {
	input := "data: {\"foo\":\"bar\"}"
	r := NewReader(bytes.NewReader([]byte(input)))

	ev, err := r.ReadEvent()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(ev.Data) != "{\"foo\":\"bar\"}" {
		t.Fatalf("unexpected data: %s", string(ev.Data))
	}

	_, err = r.ReadEvent()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

func TestEncodeEvent(t *testing.T) {
	ev := &Event{
		Type: "delta",
		ID:   "1",
		Data: []byte("foo\nbar"),
	}
	encoded := EncodeEvent(ev)
	expected := "event: delta\nid: 1\ndata: foo\ndata: bar\n\n"
	if string(encoded) != expected {
		t.Fatalf("expected %q, got %q", expected, string(encoded))
	}
}
