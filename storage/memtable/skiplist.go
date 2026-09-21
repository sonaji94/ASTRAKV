package memtable

import "math/rand/v2"

// skiplist is an in-memory sorted structure supporting O(log n) insert, get,
// and ordered forward iteration. It is the backing store of the MemTable.
// Deletions are represented as tombstones (a flag on the entry), never as
// physical removal, because removal must stay visible until compaction.
//
// The list is a classic multilevel singly-linked structure:
//
//   L3  head ────────────────────────────► node(d) ────► nil
//   L2  head ─────────► node(b) ─────────► node(d) ────► nil
//   L1  head ───► node(a) ─► node(b) ─────► node(d) ────► nil
//   L0  head ───► node(a) ─► node(b) ─► node(c) ─► node(d) ────► nil
//
// Level-0 is a fully linked, sorted list; higher levels are "express lanes"
// that let a search skip large spans. Each node's height is randomized so the
// expected search cost is O(log n).
type skiplist struct {
	head      *node
	maxLevel  int
	towerProb uint32
}

// node is one keyed entry. value is only meaningful when tombstone is false.
type node struct {
	key       string
	value     string
	tombstone bool
	next      []*node
}

// entry is what lookups return, carrying tombstones distinctly from absence.
type entry struct {
	value     string
	tombstone bool
	ok        bool
}

func newSkiplist() *skiplist {
	return &skiplist{
		head:      &node{next: make([]*node, defaultMaxLevel)},
		maxLevel:  defaultMaxLevel,
		towerProb: 4, // P(level up) = 1/4
	}
}

const defaultMaxLevel = 24

func randomLevel(maxLevel int) int {
	level := 1
	for rand.Uint32()%4 == 0 && level < maxLevel {
		level++
	}
	return level
}

// set inserts or replaces key. tombstone=true records a deletion marker.
func (s *skiplist) set(key, value string, tombstone bool) {
	prevs := make([]*node, s.maxLevel)
	x := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		prevs[i] = x
	}
	x = x.next[0]
	if x != nil && x.key == key {
		x.value = value
		x.tombstone = tombstone
		return
	}

	newNode := &node{
		key:       key,
		value:     value,
		tombstone: tombstone,
		next:      make([]*node, randomLevel(s.maxLevel)),
	}
	for i := 0; i < len(newNode.next); i++ {
		newNode.next[i] = prevs[i].next[i]
		prevs[i].next[i] = newNode
	}
}

// get returns the entry for key, or ok=false when absent.
func (s *skiplist) get(key string) entry {
	x := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && x.key == key {
		return entry{value: x.value, tombstone: x.tombstone, ok: true}
	}
	return entry{ok: false}
}

// first returns the entry with the smallest key, or ok=false when empty.
func (s *skiplist) first() (string, entry) {
	x := s.head.next[0]
	if x == nil {
		return "", entry{ok: false}
	}
	return x.key, entry{value: x.value, tombstone: x.tombstone, ok: true}
}

// next advances a bare node to its successor on level 0 (ordered iteration).
func (s *skiplist) next(cur *node) *node {
	return cur.next[0]
}

// nodeAt returns the first node whose key >= start, or nil.
func (s *skiplist) lowerBound(start string) *node {
	x := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < start {
			x = x.next[i]
		}
	}
	return x.next[0]
}

// stats returns live (non-tombstone) and total entry counts.
func (s *skiplist) stats() (live, total int) {
	for x := s.head.next[0]; x != nil; x = x.next[0] {
		total++
		if !x.tombstone {
			live++
		}
	}
	return live, total
}