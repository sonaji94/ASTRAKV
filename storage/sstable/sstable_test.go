package sstable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func write(t *testing.T, path string, entries []Entry) {
	t.Helper()
	w, err := NewWriter(path, defaultBlockTarget)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatalf("add %q: %v", e.Key, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, path string) *Reader {
	t.Helper()
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestWriteReadRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sst")
	entries := []Entry{
		{Key: "apple", Value: "100"},
		{Key: "banana", Value: "200"},
		{Key: "cat", Value: "300"},
		{Key: "dog", Deleted: true},          // tombstone, no value
		{Key: "echidna", Value: ""},          // empty value is a legal state
		{Key: "zebra", Value: "some value"},
	}
	write(t, path, entries)

	r := read(t, path)
	defer r.Close()

	if r.EntryCount() != 6 {
		t.Fatalf("expected 6 entries, got %d", r.EntryCount())
	}

	checks := []struct {
		key     string
		want    string
		deleted bool
	}{}
	_ = checks

	for _, e := range []Entry{entries[0], entries[5]} {
		got, ok, err := r.Get(e.Key)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || got != e {
			t.Fatalf("get(%q) = %+v ok=%v, want %+v", e.Key, got, ok, e)
		}
	}
}

func TestGetEachEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "each.sst")
	entries := []Entry{
		{Key: "apple", Value: "100"},
		{Key: "cat", Value: "300"},
		{Key: "dog", Deleted: true},
		{Key: "echidna", Value: ""},
	}
	write(t, path, entries)

	r := read(t, path)
	defer r.Close()

	for _, want := range entries {
		got, ok, err := r.Get(want.Key)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || got.Key != want.Key || got.Value != want.Value || got.Deleted != want.Deleted {
			t.Fatalf("get(%q) = %+v ok=%v, want %+v", want.Key, got, ok, want)
		}
	}
}

func TestGetMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "miss.sst")
	write(t, path, []Entry{{Key: "apple", Value: "1"}, {Key: "cat", Value: "3"}})

	r := read(t, path)
	defer r.Close()

	for _, miss := range []string{"abacus", "bat", "dog", "zebra"} {
		if _, ok, err := r.Get(miss); err != nil || ok {
			t.Fatalf("get(%q): expected not-found (err=%v)", miss, err)
		}
	}
}

func TestGetEmptyTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.sst")
	w, err := NewWriter(path, defaultBlockTarget)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := read(t, path)
	defer r.Close()
	if r.EntryCount() != 0 || r.BlockCount() != 0 {
		t.Fatalf("expected empty table, got entries=%d blocks=%d", r.EntryCount(), r.BlockCount())
	}
	if _, ok, err := r.Get("anything"); err != nil || ok {
		t.Fatalf("expected not-found on empty table (err=%v)", err)
	}
}

func TestMultiBlockReads(t *testing.T) {
	// Tiny block target forces many blocks.
	path := filepath.Join(t.TempDir(), "many.sst")
	var entries []Entry
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%03d", i*2) // gaps so misses land between keys
		entries = append(entries, Entry{Key: key, Value: fmt.Sprintf("v%d", i)})
	}

	w, err := NewWriter(path, 64)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := read(t, path)
	defer r.Close()
	if r.BlockCount() < 2 {
		t.Fatalf("expected multiple blocks, got %d", r.BlockCount())
	}

	for i, want := range entries {
		got, ok, err := r.Get(want.Key)
		if err != nil || !ok || got != want {
			t.Fatalf("get(%q) = %+v ok=%v err=%v, want %+v", want.Key, got, ok, err, want)
		}
		// key+1 does not exist but falls inside a block
		miss := fmt.Sprintf("key-%03d", i*2+1)
		if _, ok, err := r.Get(miss); err != nil || ok {
			t.Fatalf("get(%q): expected miss (err=%v)", miss, err)
		}
	}
}

func TestOutOfOrderRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ooo.sst")
	w, err := NewWriter(path, defaultBlockTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer w.f.Close()
	if err := w.Add(Entry{Key: "zebra", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(Entry{Key: "apple", Value: "2"}); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("expected ErrOutOfOrder, got %v", err)
	}
}

func TestIteratorSortedAcrossBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iter.sst")
	keys := []string{"banana", "cherry", "apple", "dragonfruit", "elderberry", "fig"}
	sort.Strings(keys)
	var entries []Entry
	for _, k := range keys {
		entries = append(entries, Entry{Key: k, Value: k})
	}

	w, err := NewWriter(path, 24)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := read(t, path)
	defer r.Close()

	it, err := r.NewIterator()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for it.Valid() {
		got = append(got, it.Entry().Key)
		if err := it.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(keys) {
		t.Fatalf("expected %v, got %v", keys, got)
	}
}

func TestIteratorIncludesTombstones(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tombiter.sst")
	w, err := NewWriter(path, defaultBlockTarget)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(Entry{Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(Entry{Key: "b", Deleted: true}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := read(t, path)
	defer r.Close()
	it, err := r.NewIterator()
	if err != nil {
		t.Fatal(err)
	}
	it.Next()
	if !it.Valid() || it.Entry().Key != "b" || !it.Entry().Deleted {
		t.Fatalf("expected tombstone 'b' at iterator position 1, got %+v", it.Entry())
	}
}

func TestDataBlockCorruptionDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "datacorrupt.sst")
	write(t, path, []Entry{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}})

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[10] ^= 0x08 // flip a bit inside the first data block (before footer/inplace)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	r := read(t, path)
	defer r.Close()
	if _, _, err := r.Get("a"); !errors.Is(unwrap(err), ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on data block read, got %v", err)
	}
}

func TestIndexCorruptionDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idxcorrupt.sst")
	write(t, path, []Entry{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}})

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Footer sits in the last 32 bytes; flip a byte just before it (index).
	raw[len(raw)-footerSize-5] ^= 0x40
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on open, got %v", err)
	}
}

func TestBadMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "badmagic.sst")
	if err := os.WriteFile(path, []byte("garbage here, not a table at all"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, ErrBadMagic) {
		t.Fatalf("expected ErrBadMagic, got %v", err)
	}
}

// unwrap digs through fmt-wrapped errors to find the sentinel.
func unwrap(err error) error {
	for err != nil {
		if errors.Is(err, ErrCorrupt) {
			return ErrCorrupt
		}
		err = errors.Unwrap(err)
	}
	return nil
}