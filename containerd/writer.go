package containerd

import (
	"bytes"
	"fmt"
)

type LineWriter interface {
	WriteLine(line string)
}

type Writer struct {
	buf    []byte
	prefix string
	quiet  bool
	live   LineWriter
}

func (p *Writer) Write(data []byte) (int, error) {
	p.buf = append(p.buf, data...)
	for {
		idx := bytes.IndexByte(p.buf, '\n')
		if idx < 0 {
			break
		}
		line := p.buf[:idx]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if !p.quiet {
			text := p.prefix + string(line)
			if p.live != nil {
				p.live.WriteLine(text)
			} else {
				stdoutMu.Lock()
				fmt.Println(text)
				stdoutMu.Unlock()
			}
		}
		p.buf = p.buf[idx+1:]
	}
	return len(data), nil
}
