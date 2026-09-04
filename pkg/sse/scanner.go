package sse

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

type Event struct {
	Type string
	Data []byte
	ID   string
	Raw  []byte
}

type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		br: bufio.NewReaderSize(r, 64*1024),
	}
}

func (r *Reader) ReadEvent() (*Event, error) {
	var (
		eventType string
		id        string
		dataBuf   bytes.Buffer
		hasData   bool
	)

	for {
		lineBytes, isPrefix, err := r.br.ReadLine()
		if err != nil {
			if err == io.EOF {
				if hasData || eventType != "" || id != "" {
					return &Event{
						Type: eventType,
						Data: dataBuf.Bytes(),
						ID:   id,
					}, nil
				}
				return nil, io.EOF
			}
			return nil, err
		}

		fullLine := lineBytes
		for isPrefix {
			var cont []byte
			cont, isPrefix, err = r.br.ReadLine()
			if err != nil {
				return nil, err
			}
			fullLine = append(fullLine, cont...)
		}

		if len(fullLine) == 0 {
			if hasData || eventType != "" || id != "" {
				return &Event{
					Type: eventType,
					Data: dataBuf.Bytes(),
					ID:   id,
				}, nil
			}
			continue
		}

		if fullLine[0] == ':' {
			continue
		}

		colonIdx := bytes.IndexByte(fullLine, ':')
		var field, value []byte
		if colonIdx == -1 {
			field = fullLine
			value = nil
		} else {
			field = fullLine[:colonIdx]
			value = fullLine[colonIdx+1:]
			if len(value) > 0 && value[0] == ' ' {
				value = value[1:]
			}
		}

		fieldName := string(field)
		switch fieldName {
		case "event":
			eventType = string(value)
		case "data":
			if hasData {
				dataBuf.WriteByte('\n')
			}
			dataBuf.Write(value)
			hasData = true
		case "id":
			id = string(value)
		}
	}
}

func EncodeEvent(ev *Event) []byte {
	var buf bytes.Buffer
	if ev.Type != "" {
		buf.WriteString("event: ")
		buf.WriteString(ev.Type)
		buf.WriteByte('\n')
	}
	if ev.ID != "" {
		buf.WriteString("id: ")
		buf.WriteString(ev.ID)
		buf.WriteByte('\n')
	}
	if len(ev.Data) > 0 {
		lines := bytes.Split(ev.Data, []byte("\n"))
		for _, line := range lines {
			buf.WriteString("data: ")
			buf.Write(line)
			buf.WriteByte('\n')
		}
	} else {
		buf.WriteString("data:\n")
	}
	buf.WriteByte('\n')
	return buf.Bytes()
}

func EncodeData(data []byte) []byte {
	var buf bytes.Buffer
	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		buf.WriteString("data: ")
		buf.Write(line)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	return buf.Bytes()
}

func EncodeDone() []byte {
	return []byte("data: [DONE]\n\n")
}

func TrimLeadingWhitespace(s string) string {
	return strings.TrimLeft(s, " \t")
}
