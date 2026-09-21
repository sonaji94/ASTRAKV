package memtable

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

func collect(t *testing.T, m *MemTable) []string {
	t.Helper()
	var keys []string
	it := m.NewIterator()
	for it.Valid() {
		keys = append(keys, it.Key())
		it.Next()
	}
	return keys
}

func TestMemTableOrderedIteration(t *testing.T) {
	m := New()
	keys := []string{"dog", "cat", "apple", "zebra", "banana"}
	for _, k := range keys {
		m.Put(k, strings.ToUpper(k))
	}

	got := collect(t, m)
	want := []string{"apple", "banana", "cat", "dog", "zebra"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("expected sorted %v, got %v", want, got)
	}
}

func TestMemTableGetFound(t *testing.T) {
	m := New()
	m.Put("name", "Sonu")

	value, status := m.Get("name")
	if status != Found || value != "Sonu" {
		t.Fatalf("expected Found/Sonu, got %v/%q", status, value)
	}
	if m.Len() != 1 {
		t.Fatalf("expected Len 1, got %d", m.Len())
	}
}

func TestMemTableNotFound(t *testing.T) {
	m := New()
	if _, status := m.Get("name"); status != NotFound {
		t.Fatalf("expected NotFound, got %v", status)
	}
}

func TestMemTableOverwriteResurrects(t *testing.T) {
	m := New()
	m.Put("age", "21")
	m.Put("age", "22")

	value, status := m.Get("age")
	if status != Found || value != "22" {
		t.Fatalf("expected overwrite to 22, got %v/%q", status, value)
	}
	if m.Len() != 1 {
		t.Fatalf("expected Len 1 after overwrite, got %d", m.Len())
	}
}

func TestMemTableTombstone(t *testing.T) {
	m := New()
	m.Put("name", "Sonu")
	m.Delete("name")

	if value, status := m.Get("name"); status != Deleted {
		t.Fatalf("expected Deleted status, got %v/%q", status, value)
	}
	// Tombstone counts as dead for Len but still occupies space.
	if m.Len() != 0 {
		t.Fatalf("expected Len 0 with tombstone, got %d", m.Len())
	}
	if m.Entries() != 1 {
		t.Fatalf("expected 1 entry (the tombstone), got %d", m.Entries())
	}
}

func TestMemTablePutAfterDeleteRevives(t *testing.T) {
	m := New()
	m.Delete("name")
	m.Put("name", "Sonu")

	if value, status := m.Get("name"); status != Found || value != "Sonu" {
		t.Fatalf("expected put-after-delete to revive, got %v/%q", status, value)
	}
	if m.Len() != 1 {
		t.Fatalf("expected Len 1, got %d", m.Len())
	}
}

func TestMemTableIteratorStatuses(t *testing.T) {
	m := New()
	m.Put("a", "1")
	m.Delete("b")
	m.Put("c", "3")

	it := m.NewIterator()
	var statuses []Status
	for it.Valid() {
		statuses = append(statuses, it.Status())
		it.Next()
	}
	if fmt.Sprint(statuses) != fmt.Sprint([]Status{Found, Deleted, Found}) {
		t.Fatalf("expected [Found Deleted Found], got %v", statuses)
	}
}

func TestMemTableSeek(t *testing.T) {
	m := New()
	for _, k := range []string{"apple", "banana", "cat", "dog"} {
		m.Put(k, k)
	}

	var got []string
	it := m.Seek("banana")
	for it.Valid() {
		got = append(got, it.Key())
		it.Next()
	}
	if fmt.Sprint(got) != fmt.Sprint([]string{"banana", "cat", "dog"}) {
		t.Fatalf("seek failed, got %v", got)
	}

	it = m.Seek("zzz")
	if it.Valid() {
		t.Fatalf("seek past end should be invalid, got key %q", it.Key())
	}
	if it := m.Seek("apple"); !it.Valid() {
		t.Fatal("seek to first key should be valid")
	}
}

func TestBufferThresholdSeals(t *testing.T) {
	// Threshold 3: the 3rd write fills the active table, so every subsequent
	// write seals it.
	b := NewBuffer(3)
	b.Put("a", "1")
	b.Put("b", "2")
	b.Put("c", "3")
	if len(b.Sealed()) != 0 {
		t.Fatalf("expected no seal at exactly threshold, got %d", len(b.Sealed()))
	}

	b.Put("d", "4")
	if len(b.Sealed()) != 1 {
		t.Fatalf("expected 1 sealed table, got %d", len(b.Sealed()))
	}
	if b.Active().Len() != 1 || b.Sealed()[0].Len() != 3 {
		t.Fatalf("expected active=1 sealed=3, got active=%d sealed=%d", b.Active().Len(), b.Sealed()[0].Len())
	}
}

func TestBufferReadPathShadows(t *testing.T) {
	b := NewBuffer(3)
	b.Put("name", "Sonu")
	b.Put("temp", "x")
	b.Put("bench", "y")
	b.Put("age", "22") // seals table containing name/temp/bench

	// Reads consult sealed tables when not on the active table.
	if value, status := b.Get("name"); status != Found || value != "Sonu" {
		t.Fatalf("expected sealed table read to find name, got %v/%q", status, value)
	}
	if _, status := b.Get("nope"); status != NotFound {
		t.Fatalf("expected NotFound, got %v", status)
	}
}

func TestBufferTombstoneShadowsSealed(t *testing.T) {
	b := NewBuffer(1)
	b.Put("name", "Sonu") // active full
	b.Delete("name")       // seals, then tombstones in new active table

	// Newer table's tombstone must hide the older sealed value.
	if value, status := b.Get("name"); status != Deleted {
		t.Fatalf("expected tombstone to shadow sealed value, got %v/%q", status, value)
	}
}

func TestBufferReset(t *testing.T) {
	b := NewBuffer(1)
	for i := 0; i < 4; i++ {
		b.Put(fmt.Sprintf("k%d", i), "v")
	}
	if len(b.Sealed()) != 3 {
		t.Fatalf("expected 3 sealed tables (active holds the 4th), got %d", len(b.Sealed()))
	}
	if b.Active().Len() != 1 {
		t.Fatalf("expected active to hold 1 entry, got %d", b.Active().Len())
	}

	b.Reset(b.Sealed())
	if len(b.Sealed()) != 0 {
		t.Fatalf("expected no sealed tables after reset, got %d", len(b.Sealed()))
	}
}

// TestSkiplistManyRandomKeys exercises the skip list with random inserts and
// verifies both lookups and sorted iteration — the property a MemTable relies on.
func TestSkiplistManyRandomKeys(t *testing.T) {
	const n = 5000
	s := newSkiplist()

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%06d-value-%d", i*7 % n, i)
		s.set(k, k, false)
	}
	if live, total := s.stats(); live != n || total != n {
		t.Fatalf("expected %d entries, got live=%d total=%d", n, live, total)
	}

	keys := make([]string, 0, n)
	x := s.head.next[0]
	for x != nil {
		keys = append(keys, x.key)
		x = x.next[0]
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatal("iteration is not sorted")
	}

	for i := 0; i < n; i++ {
		want := fmt.Sprintf("key-%06d-value-%d", i*7 % n, i)
		if e := s.get(want); !e.ok || e.tombstone || e.value != want {
			t.Fatalf("get(%q) failed", want)
		}
	}
}