package util

import (
	"io"
	"sync"
)

type ProgressReader struct {
	reader   io.Reader
	total    int64
	current  int64
	mu       sync.Mutex
	progress float64
}

func NewProgressReader(reader io.Reader, total int64) *ProgressReader {
	return &ProgressReader{reader: reader, total: total}
}

func (pr *ProgressReader) Close() error {
	if closer, ok := pr.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	n, err = pr.reader.Read(p)
	pr.current += int64(n)
	if pr.total > 0 {
		pr.progress = float64(pr.current) / float64(pr.total)
	}
	return n, err
}

func (pr *ProgressReader) Progress() float64 {
	return pr.progress
}

type ProgressWriter struct {
	writer   io.Writer
	total    int64
	current  int64
	mu       sync.Mutex
	progress float64
}

func NewProgressWriter(writer io.Writer, total int64) *ProgressWriter {
	return &ProgressWriter{writer: writer, total: total}
}

func (pr *ProgressWriter) Close() error {
	if closer, ok := pr.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (pr *ProgressWriter) Write(p []byte) (n int, err error) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	n, err = pr.writer.Write(p)
	pr.current += int64(n)
	if pr.total > 0 {
		pr.progress = float64(pr.current) / float64(pr.total)
	}
	return n, err
}

func (pr *ProgressWriter) Progress() float64 {
	return pr.progress
}
