package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNutsDBAtomicBatchAndReopen(t *testing.T) {
	dir := t.TempDir()
	backend, err := OpenNutsDB(dir)
	if err != nil {
		t.Fatal(err)
	}

	storage := NewStore(backend)
	if _, err := storage.Read([]byte("missing")); !errors.Is(err, ErrNotFoundKey) {
		t.Fatalf("missing key error: %v", err)
	}

	entries := []Entry{
		{[]byte("block/1"), []byte("block")},
		{[]byte("cursor"), []byte("1")},
	}
	if err := storage.WriteBatch(entries); err != nil {
		t.Fatal(err)
	}

	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenNutsDB(dir)
	if err != nil {
		t.Fatal(err)
	}

	restored := NewStore(reopened)
	defer restored.Close()
	value, err := restored.Read([]byte("cursor"))
	if err != nil || string(value) != "1" {
		t.Fatal("cursor did not survive restart")
	}

	scanned, err := restored.Scan([]byte("block/"))
	if err != nil || len(scanned) != 1 {
		t.Fatal("prefix recovery failed")
	}

	if err := restored.WriteBatch([]Entry{
		{[]byte("cursor"), []byte("2")},
		{nil, []byte("invalid")},
	}); err == nil {
		t.Fatal("invalid batch succeeded")
	}

	value, _ = restored.Read([]byte("cursor"))
	if string(value) != "1" {
		t.Fatal("failed batch partially updated cursor")
	}
}

func TestNotifyReadCancellationAndClose(t *testing.T) {
	storage := NewStore(NewMemoryDB())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := storage.NotifyReadContext(ctx, []byte("missing")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := storage.NotifyReadContext(context.Background(), []byte("later"))
		result <- err
	}()
	storage.Write([]byte("later"), []byte("value"))
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	storage.Close()
	if _, err := storage.Read([]byte("later")); !errors.Is(err, ErrClosed) {
		t.Fatal("closed store accepted read")
	}
}
