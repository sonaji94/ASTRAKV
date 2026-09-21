// Package memtable implements the in-memory sorted write buffer of AstraKV.
//
// The MemTable is a skip-list-backed structure that accepts writes (which are
// already durably logged by the WAL) and answers reads in sorted order. Two
// properties make it LSM-ready:
//
//   - Tombstones: Delete marks an entry, it does not remove it. Absence and
//     "deleted" are distinct, so a deletion stays visible to readers until a
//     later compaction drops it.
//   - Rotation: the Buffer holds one active MemTable that accepts writes and
//     any number of sealed (read-only) MemTables that are past the flush
//     threshold and waiting to be written to SSTables.
package memtable

// Status classifies the result of a lookup.
type Status int

const (
	// NotFound means no entry exists for the key above.
	NotFound Status = iota
	// Found means the key has a live value; getValue is valid.
	Found
	// Deleted means the key carries a tombstone: it was written and then
	// deleted, and must not be resurrected by older data below.
	Deleted
)

func (s Status) String() string {
	switch s {
	case NotFound:
		return "NotFound"
	case Found:
		return "Found"
	case Deleted:
		return "Deleted"
	default:
		return "Status(?)"
	}
}

// MemTable is a single mutable (or sealed) in-memory table.
type MemTable struct {
	list *skiplist
}

// New returns an empty MemTable.
func New() *MemTable {
	return &MemTable{list: newSkiplist()}
}

// Put stores a live value under key, replacing any previous entry (including
// a tombstone).
func (m *MemTable) Put(key, value string) {
	m.list.set(key, value, false)
}

// Delete records a tombstone under key, replacing any previous live value.
func (m *MemTable) Delete(key string) {
	m.list.set(key, "", true)
}

// Get classifies key as NotFound, Found, or Deleted. A live value is returned
// only for Found.
func (m *MemTable) Get(key string) (string, Status) {
	e := m.list.get(key)
	switch {
	case !e.ok:
		return "", NotFound
	case e.tombstone:
		return "", Deleted
	default:
		return e.value, Found
	}
}

// Len returns the number of live (non-tombstone) keys.
func (m *MemTable) Len() int {
	live, _ := m.list.stats()
	return live
}

// Entries returns the total number of entries including tombstones. This is
// the quantity that drives the flush threshold, because tombstones still
// occupy space on disk and in memory.
func (m *MemTable) Entries() int {
	_, total := m.list.stats()
	return total
}

// Empty reports whether the table has no entries at all.
func (m *MemTable) Empty() bool {
	return m.Entries() == 0
}

// Iterator walks entries in ascending key order. Both live values and
// tombstones are visited; callers distinguish them with Status().
type Iterator struct {
	mt *MemTable
	at *node
}

// NewIterator returns an iterator positioned at the first entry (if any).
func (m *MemTable) NewIterator() *Iterator {
	key, e := m.list.first()
	it := &Iterator{mt: m}
	if e.ok {
		it.at = m.list.lowerBound(key)
	}
	return it
}

// Seek moves the iterator to the first entry with key >= target.
func (m *MemTable) Seek(target string) *Iterator {
	return &Iterator{mt: m, at: m.list.lowerBound(target)}
}

// Valid reports whether the iterator is within the table.
func (it *Iterator) Valid() bool { return it.at != nil }

// Next advances to the following entry.
func (it *Iterator) Next() {
	if it.at != nil {
		it.at = it.mt.list.next(it.at)
	}
}

// Key returns the current entry's key.
func (it *Iterator) Key() string { return it.at.key }

// Value returns the current entry's value (only meaningful when Status() is
// Found).
func (it *Iterator) Value() string { return it.at.value }

// Status returns Found for live entries and Deleted for tombstones.
func (it *Iterator) Status() Status {
	if it.at.tombstone {
		return Deleted
	}
	return Found
}

// Buffer holds one active MemTable plus sealed tables waiting to be flushed.
// Writes always go to the active table; reads consult the active table first,
// then sealed tables from newest to oldest.
type Buffer struct {
	active    *MemTable
	sealed    []*MemTable
	threshold int
}

// NewBuffer returns a Buffer that seals a full active table once it reaches
// threshold entries.
func NewBuffer(threshold int) *Buffer {
	return &Buffer{
		active:    New(),
		threshold: threshold,
	}
}

// Put writes through to the active table, sealing it first if it is full.
func (b *Buffer) Put(key, value string) {
	if b.active.Entries() >= b.threshold {
		b.seal()
	}
	b.active.Put(key, value)
}

// Delete writes a tombstone through to the active table, sealing first if
// full.
func (b *Buffer) Delete(key string) {
	if b.active.Entries() >= b.threshold {
		b.seal()
	}
	b.active.Delete(key)
}

// Get searches the active table first, then sealed tables newest-first. An
// entry found in a newer table shadows older tables; this is what makes a
// tombstone hide a value in a sealed table below.
func (b *Buffer) Get(key string) (string, Status) {
	if value, status := b.active.Get(key); status != NotFound {
		return value, status
	}
	for i := len(b.sealed) - 1; i >= 0; i-- {
		if value, status := b.sealed[i].Get(key); status != NotFound {
			return value, status
		}
	}
	return "", NotFound
}

// seal moves the active table into the sealed set and starts a fresh one.
func (b *Buffer) seal() {
	if b.active.Empty() {
		return
	}
	b.sealed = append(b.sealed, b.active)
	b.active = New()
}

// Sealed returns the read-only tables waiting to be flushed, oldest first.
func (b *Buffer) Sealed() []*MemTable { return b.sealed }

// Active returns the table currently accepting writes.
func (b *Buffer) Active() *MemTable { return b.active }

// Reset drops all sealed tables (e.g. after a flush wrote them to SSTables).
func (b *Buffer) Reset(flushed []*MemTable) {
	kept := b.sealed[:0]
	for _, t := range b.sealed {
		found := false
		for _, f := range flushed {
			if t == f {
				found = true
				break
			}
		}
		if !found {
			kept = append(kept, t)
		}
	}
	b.sealed = kept
}