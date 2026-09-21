package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openLogForRead(t *testing.T, path string) []Record {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open read handle: %v", err)
	}
	defer f.Close()
	records, err := Replay(f)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return records
}

func TestAppendReplayRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	records := []Record{
		{Sequence: 1, Op: OpPut, Key: "name", Value: "Sonu"},
		{Sequence: 2, Op: OpPut, Key: "age", Value: "22"},
		{Sequence: 3, Op: OpDelete, Key: "age"},
	}
	for _, rec := range records {
		if err := l.Append(rec); err != nil {
			t.Fatalf("append seq %d: %v", rec.Sequence, err)
		}
	}
	l.Close()

	got := openLogForRead(t, path)
	if len(got) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(got))
	}
	for i, rec := range records {
		if got[i] != rec {
			t.Fatalf("record %d mismatch:\n got %+v\nwant %+v", i, got[i], rec)
		}
	}
}

func TestReplayBlankLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()

	records := openLogForRead(t, path)
	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}
}

func TestSequenceContinuesAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seq.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(Record{Sequence: 1, Op: OpPut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	l.Close()

	// A second Open starts fresh; caller is responsible for seeding the
	// sequence from replay (the storage layer does this).
	l2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := l2.NextSequence(); got != 1 {
		t.Fatalf("expected fresh log to start at 1, got %d", got)
	}
	l2.Close()
}

func TestSequenceOutOfOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ooo.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if err := l.Append(Record{Sequence: 1, Op: OpPut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(Record{Sequence: 3, Op: OpPut, Key: "b", Value: "2"}); !errors.Is(err, ErrSequenceOutOfOrder) {
		t.Fatalf("expected ErrSequenceOutOfOrder, got %v", err)
	}
	// No partial state: the next valid append still works.
	if err := l.Append(Record{Sequence: 2, Op: OpPut, Key: "b", Value: "2"}); err != nil {
		t.Fatalf("append after rejected record: %v", err)
	}
}

func TestTornTailIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(Record{Sequence: 1, Op: OpPut, Key: "hello", Value: "world"}); err != nil {
		t.Fatal(err)
	}
	l.Close()

	// Simulate a crash mid-write: a header for a record that was never fully
	// written (key declared but bytes truncated).
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	partial := []byte{OpPut, 0, 0, 0, 0, 0, 0, 0, 2, 0, 5, 0, 0, 0, 3}
	f.Write(partial)
	f.Close()

	fr, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	records, err := Replay(fr)
	fr.Close()
	if !errors.Is(err, ErrTornTail) {
		t.Fatalf("expected ErrTornTail, got %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 complete record, got %d", len(records))
	}
	if records[0].Key != "hello" {
		t.Fatalf("expected replay of 'hello', got %q", records[0].Key)
	}
}

func TestCorruptionDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(Record{Sequence: 1, Op: OpPut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	l.Close()

	// Flip a bit in the middle of the payload so the checksum no longer holds.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[16] ^= 0x80
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	fr, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fr.Close()
	if _, err := Replay(fr); err == nil {
		t.Fatal("expected corruption error, got nil")
	}
}

func TestChecksumTooShort(t *testing.T) {
	// A record whose declared payload fits but whose checksum is truncated is
	// indistinguishable from a torn tail.
	good := encode(Record{Sequence: 1, Op: OpPut, Key: "k", Value: "v"})
	cut := good[:len(good)-2]
	records, err := Replay(bytes.NewReader(cut))
	if !errors.Is(err, ErrTornTail) {
		t.Fatalf("expected ErrTornTail, got %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	records := []Record{
		{Sequence: 1, Op: OpPut, Key: "name", Value: "Sonu"},
		{Sequence: 2, Op: OpPut, Key: "", Value: ""},            // empty key
		{Sequence: 3, Op: OpDelete, Key: "name", Value: ""},     // delete has no value
		{Sequence: 4, Op: OpPut, Key: "x", Value: "some value"}, // value with spaces
	}
	var buf bytes.Buffer
	for _, rec := range records {
		buf.Write(encode(rec))
	}
	got, err := Replay(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(got))
	}
	for i, rec := range records {
		if got[i] != rec {
			t.Fatalf("record %d mismatch:\n got %+v\nwant %+v", i, got[i], rec)
		}
	}
}