package logging

import (
	"os"
	"testing"
	"time"
)

type fakeStatWriter struct {
	mode os.FileMode
}

func (w fakeStatWriter) Write(data []byte) (int, error) {
	return len(data), nil
}

func (w fakeStatWriter) Stat() (os.FileInfo, error) {
	return fakeFileInfo(w), nil
}

type fakeFileInfo struct {
	mode os.FileMode
}

func (info fakeFileInfo) Name() string       { return "fake" }
func (info fakeFileInfo) Size() int64        { return 0 }
func (info fakeFileInfo) Mode() os.FileMode  { return info.mode }
func (info fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (info fakeFileInfo) IsDir() bool        { return false }
func (info fakeFileInfo) Sys() any           { return nil }

type discardStatWriter struct{}

func (discardStatWriter) Write(data []byte) (int, error) {
	return len(data), nil
}

func (discardStatWriter) Stat() (os.FileInfo, error) {
	return fakeFileInfo{mode: 0}, nil
}

func TestColorEnabled(t *testing.T) {
	t.Run("non tty writer disables color", func(t *testing.T) {
		if colorEnabledWithLookup(discardStatWriter{}, func(string) (string, bool) { return "", false }) {
			t.Fatal("expected color to be disabled for non-tty writer")
		}
	})

	t.Run("char device enables color", func(t *testing.T) {
		if !colorEnabledWithLookup(fakeStatWriter{mode: os.ModeCharDevice}, func(string) (string, bool) { return "", false }) {
			t.Fatal("expected color to be enabled for tty-like writer")
		}
	})

	t.Run("no color env disables color", func(t *testing.T) {
		if colorEnabledWithLookup(fakeStatWriter{mode: os.ModeCharDevice}, func(string) (string, bool) { return "1", true }) {
			t.Fatal("expected NO_COLOR to disable color")
		}
	})
}
