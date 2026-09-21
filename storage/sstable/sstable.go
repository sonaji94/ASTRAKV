// Package sstable implements immutable, sorted, disk-backed tables for
// AstraKV. A table partitions its entries into data blocks, records the first
// key + offset + length + CRC of every block in an index, and ends with a
// fixed-size footer:
//
//	┌────────────────────────────┐
//	│ magic "AKV1"               │
//	│ data block   0             │  entries + trailing CRC32
//	│ data block   1             │
//	│ ...                        │
//	│ data block   N             │
//	│ index                      │  [firstKey, offset, len, CRC] per block
//	│ footer (32 bytes)          │  30-byte metadata + magic
//	└────────────────────────────┘
//
// Point reads use the in-memory index to binary-search for the block that may
// hold a key, read just that block, verify its CRC, then scan it linearly.
// Since tables are immutable, writes happen once at build time (Writer) and
// everything afterwards is read-only (Reader).
package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	magic   = "AKV1"
	footerSize = 32
	entryHeaderSize  = 7  // keyLen(2) + valueLen(4) + flags(1)
	defaultBlockTarget = 4096
)

// flagDeleted marks a tombstone entry in flags.
const flagDeleted byte = 1 << 0

// Errors returned by the package.
var (
	ErrOutOfOrder = errors.New("sstable: keys must be written in strictly increasing order")
	ErrCorrupt    = errors.New("sstable: checksum mismatch")
	ErrBadMagic   = errors.New("sstable: not an AstraKV table (bad magic)")
)

// Entry is one row stored in a table.
type Entry struct {
	Key     string
	Value   string
	Deleted bool // tombstone: a key written then deleted
}

// indexEntry locates one data block.
type indexEntry struct {
	firstKey string
	offset   uint64
	length   uint32
	crc      uint32
}

// footer is the fixed-size trailer.
type footer struct {
	indexOffset uint64
	indexLen    uint32
	indexCRC    uint32
	blockCount  uint32
	entryCount  uint64
}

func encodeFooter(f footer) [footerSize]byte {
	var buf [footerSize]byte
	// indexOffset u64, indexLen u32, indexCRC u32, blockCount u32, entryCount u64, magic 4B
	binary.BigEndian.PutUint64(buf[0:8], f.indexOffset)
	binary.BigEndian.PutUint32(buf[8:12], f.indexLen)
	binary.BigEndian.PutUint32(buf[12:16], f.indexCRC)
	binary.BigEndian.PutUint32(buf[16:20], f.blockCount)
	binary.BigEndian.PutUint64(buf[20:28], f.entryCount)
	copy(buf[28:32], magic)
	return buf
}

func decodeFooter(buf []byte) (footer, error) {
	if len(buf) < footerSize {
		return footer{}, ErrCorrupt
	}
	if string(buf[28:32]) != magic {
		return footer{}, ErrBadMagic
	}
	return footer{
		indexOffset: binary.BigEndian.Uint64(buf[0:8]),
		indexLen:    binary.BigEndian.Uint32(buf[8:12]),
		indexCRC:    binary.BigEndian.Uint32(buf[12:16]),
		blockCount:  binary.BigEndian.Uint32(buf[16:20]),
		entryCount:  binary.BigEndian.Uint64(buf[20:28]),
	}, nil
}

// ---------------------------------------------------------------------------
// Writer

// Writer builds an immutable table. Entries must be added in strictly
// increasing key order.
type Writer struct {
	f        *os.File
	path     string
	blockTarget int

	offset      uint64
	blockCount  uint32
	entryCount  uint64
	curBlock    []byte
	curFirstKey string
	lastKey     string
	indexBuf    []byte
}

// NewWriter creates a table at path.
func NewWriter(path string, blockTarget int) (*Writer, error) {
	if blockTarget <= 0 {
		blockTarget = defaultBlockTarget
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sstable: create: %w", err)
	}
	return &Writer{f: f, path: path, blockTarget: blockTarget}, nil
}

// Add appends one entry. Keys must be strictly increasing.
func (w *Writer) Add(e Entry) error {
	if w.lastKey != "" && e.Key <= w.lastKey {
		return ErrOutOfOrder
	}
	blob := encodeEntry(e)

	// Start a new block when the current one exceeds the target. Always keep
	// at least one entry per block.
	if len(w.curBlock) > 0 && len(w.curBlock)+len(blob) > w.blockTarget {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	if len(w.curBlock) == 0 {
		w.curFirstKey = e.Key
	}
	w.curBlock = append(w.curBlock, blob...)
	w.lastKey = e.Key
	w.entryCount++
	return nil
}

// flushBlock writes the accumulated block plus its CRC and records it in the
// index.
func (w *Writer) flushBlock() error {
	if len(w.curBlock) == 0 {
		return nil
	}
	sum := crc32.ChecksumIEEE(w.curBlock)
	block := make([]byte, len(w.curBlock), len(w.curBlock)+4)
	copy(block, w.curBlock)
	block = binary.BigEndian.AppendUint32(block, sum)

	w.indexBuf = appendIndexEntry(w.indexBuf, indexEntry{
		firstKey: w.curFirstKey,
		offset:   w.offset,
		length:   uint32(len(block)),
		crc:      sum,
	})

	if _, err := w.f.Write(block); err != nil {
		return fmt.Errorf("sstable: write block: %w", err)
	}
	w.offset += uint64(len(block))
	w.blockCount++
	w.curBlock = w.curBlock[:0]
	w.curFirstKey = ""
	return nil
}

// Close flushes the final block, writes the index and footer, fsyncs, and
// closes the file.
func (w *Writer) Close() error {
	if err := w.flushBlock(); err != nil {
		return err
	}

	indexCRC := crc32.ChecksumIEEE(w.indexBuf)
	indexOffset := w.offset
	if len(w.indexBuf) > 0 {
		if _, err := w.f.Write(w.indexBuf); err != nil {
			return fmt.Errorf("sstable: write index: %w", err)
		}
		w.offset += uint64(len(w.indexBuf))
	}

	foot := encodeFooter(footer{
		indexOffset: indexOffset,
		indexLen:    uint32(len(w.indexBuf)),
		indexCRC:    indexCRC,
		blockCount:  w.blockCount,
		entryCount:  w.entryCount,
	})
	if _, err := w.f.Write(foot[:]); err != nil {
		return fmt.Errorf("sstable: write footer: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("sstable: fsync: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("sstable: close: %w", err)
	}
	return nil
}

func encodeEntry(e Entry) []byte {
	buf := make([]byte, entryHeaderSize+len(e.Key)+len(e.Value))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(e.Key)))
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(e.Value)))
	var flags byte
	if e.Deleted {
		flags |= flagDeleted
	}
	buf[6] = flags
	copy(buf[entryHeaderSize:], e.Key)
	copy(buf[entryHeaderSize+len(e.Key):], e.Value)
	return buf
}

func appendIndexEntry(dst []byte, ie indexEntry) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(ie.firstKey)))
	dst = append(dst, ie.firstKey...)
	dst = binary.BigEndian.AppendUint64(dst, ie.offset)
	dst = binary.BigEndian.AppendUint32(dst, ie.length)
	dst = binary.BigEndian.AppendUint32(dst, ie.crc)
	return dst
}

// ---------------------------------------------------------------------------
// Reader

// Reader is a read-only handle to one immutable table.
type Reader struct {
	f     *os.File
	path  string
	index []indexEntry
	foot  footer
}

// Open loads the footer and index of a table and prepares it for reads.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.Size() < footerSize {
		f.Close()
		return nil, ErrCorrupt
	}

	footBuf := make([]byte, footerSize)
	if _, err := f.ReadAt(footBuf, info.Size()-footerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	foot, err := decodeFooter(footBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	r := &Reader{f: f, path: path, foot: foot}
	if foot.indexLen > 0 {
		idx := make([]byte, foot.indexLen)
		if _, err := f.ReadAt(idx, int64(foot.indexOffset)); err != nil {
			f.Close()
			return nil, fmt.Errorf("sstable: read index: %w", err)
		}
		if crc32.ChecksumIEEE(idx) != foot.indexCRC {
			f.Close()
			return nil, ErrCorrupt
		}
		r.index, err = decodeIndex(idx)
		if err != nil {
			f.Close()
			return nil, err
		}
	}
	return r, nil
}

func decodeIndex(buf []byte) ([]indexEntry, error) {
	var idx []indexEntry
	pos := 0
	for pos < len(buf) {
		if pos+2 > len(buf) {
			return nil, ErrCorrupt
		}
		keyLen := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
		pos += 2
		if pos+keyLen+16 > len(buf) {
			return nil, ErrCorrupt
		}
		firstKey := string(buf[pos : pos+keyLen])
		pos += keyLen
		offset := binary.BigEndian.Uint64(buf[pos : pos+8])
		length := binary.BigEndian.Uint32(buf[pos+8 : pos+12])
		crc := binary.BigEndian.Uint32(buf[pos+12 : pos+16])
		pos += 16
		idx = append(idx, indexEntry{firstKey: firstKey, offset: offset, length: length, crc: crc})
	}
	return idx, nil
}

// blockFor returns the index of the block that could contain key, or -1.
func (r *Reader) blockFor(key string) int {
	lo, hi := 0, len(r.index)
	for lo < hi {
		mid := (lo + hi) / 2
		if r.index[mid].firstKey <= key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

// Get looks up key and reports whether it exists. A tombstone is returned
// with Deleted=true and an empty Value.
func (r *Reader) Get(key string) (Entry, bool, error) {
	if len(r.index) == 0 {
		return Entry{}, false, nil
	}
	bi := r.blockFor(key)
	if bi < 0 {
		return Entry{}, false, nil
	}
	entries, err := r.readBlock(bi)
	if err != nil {
		return Entry{}, false, err
	}
	for _, e := range entries {
		if e.Key == key {
			return e, true, nil
		}
		if e.Key > key {
			return Entry{}, false, nil
		}
	}
	return Entry{}, false, nil
}

func (r *Reader) readBlock(i int) ([]Entry, error) {
	if i < 0 || i >= len(r.index) {
		return nil, io.EOF
	}
	ie := r.index[i]
	buf := make([]byte, ie.length)
	if _, err := r.f.ReadAt(buf, int64(ie.offset)); err != nil {
		return nil, fmt.Errorf("sstable: read block %d: %w", i, err)
	}
	payload, crc := buf[:len(buf)-4], buf[len(buf)-4:]
	if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(crc) {
		return nil, fmt.Errorf("%w: block %d", ErrCorrupt, i)
	}
	return decodeEntries(payload)
}

func decodeEntries(buf []byte) ([]Entry, error) {
	var entries []Entry
	pos := 0
	for pos < len(buf) {
		if pos+entryHeaderSize > len(buf) {
			return nil, ErrCorrupt
		}
		keyLen := int(binary.BigEndian.Uint16(buf[pos : pos+2]))
		valLen := int(binary.BigEndian.Uint32(buf[pos+2 : pos+6]))
		flags := buf[pos+6]
		pos += entryHeaderSize
		if pos+keyLen+valLen > len(buf) {
			return nil, ErrCorrupt
		}
		entries = append(entries, Entry{
			Key:     string(buf[pos : pos+keyLen]),
			Value:   string(buf[pos+keyLen : pos+keyLen+valLen]),
			Deleted: flags&flagDeleted != 0,
		})
		pos += keyLen + valLen
	}
	return entries, nil
}

// Iterator walks entries in ascending key order across all blocks.
type Iterator struct {
	r      *Reader
	block  int
	cur    []Entry
	pos    int
	done   bool
}

// NewIterator returns an iterator over all entries.
func (r *Reader) NewIterator() (*Iterator, error) {
	it := &Iterator{r: r}
	if len(r.index) == 0 {
		it.done = true
		return it, nil
	}
	if err := it.loadBlock(0); err != nil {
		return nil, err
	}
	return it, nil
}

func (it *Iterator) loadBlock(i int) error {
	entries, err := it.r.readBlock(i)
	if err != nil {
		return err
	}
	it.block = i
	it.cur = entries
	it.pos = 0
	return nil
}

// Valid reports whether the iterator is on an entry.
func (it *Iterator) Valid() bool { return !it.done && it.block < len(it.r.index) && it.pos < len(it.cur) }

// Next advances to the next entry.
func (it *Iterator) Next() error {
	it.pos++
	if it.pos < len(it.cur) {
		return nil
	}
	it.block++
	if it.block >= len(it.r.index) {
		it.done = true
		return nil
	}
	return it.loadBlock(it.block)
}

// Entry returns the current entry.
func (it *Iterator) Entry() Entry { return it.cur[it.pos] }

// EntryCount returns the number of entries recorded in the footer.
func (r *Reader) EntryCount() uint64 { return r.foot.entryCount }

// BlockCount returns the number of data blocks.
func (r *Reader) BlockCount() int { return len(r.index) }

// Path returns the backing file path.
func (r *Reader) Path() string { return r.path }

// Close releases the file.
func (r *Reader) Close() error {
	if r.f != nil {
		err := r.f.Close()
		r.f = nil
		return err
	}
	return nil
}