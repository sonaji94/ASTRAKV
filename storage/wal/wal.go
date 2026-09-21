// Package wal implements a write-ahead log: an append-only, fsync'ed record
// file that is the source of durability for AstraKV.
//
// The rule is simple: a mutation must be durably logged BEFORE it is applied
// to memory. On startup the log is replayed to rebuild state. Crash safety is
// provided by a checksum per record and a defined torn-tail policy: a
// partially-written record at the end of the file is a crash artifact and is
// ignored; a record whose checksum fails in the middle of the log is genuine
// corruption and is reported as an error.
//
// On-disk record layout (all integers big-endian):
//
//	offset  size  field
//	0       1     op (1=PUT, 2=DELETE)
//	1       8     sequence (uint64)
//	9       2     key length (uint16)
//	11      4     value length (uint32)
//	15      k     key bytes
//	15+k    v     value bytes
//	15+k+v  4     CRC32 checksum over bytes [0, 15+k+v)
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Operation codes stored in each record.
const (
	OpPut    byte = 1
	OpDelete byte = 2
)

const headerSize = 15

// Errors returned by the package.
var (
	// ErrTornTail marks a truncated record at the end of the log. The bytes
	// read so far are valid; only a partial (crash-interrupted) record was
	// skipped.
	ErrTornTail = errors.New("wal: torn tail (truncated record ignored)")
	// ErrCorrupt marks a complete record whose checksum does not match.
	ErrCorrupt = errors.New("wal: corrupt record (checksum mismatch)")
	// ErrChecksum is returned when the trailing 4 bytes of a record are not
	// present, making the checksum unreadable.
	ErrChecksum = errors.New("wal: missing checksum")
	// ErrSequenceOutOfOrder is returned by Append when a record does not
	// continue the sequence chain.
	ErrSequenceOutOfOrder = errors.New("wal: sequence out of order")
)

// Record is a single mutation appended to the log.
type Record struct {
	Sequence uint64
	Op       byte
	Key      string
	Value    string
}

// Log is an append-only write-ahead log. All methods are safe for concurrent
// use.
type Log struct {
	mu       sync.Mutex
	f        *os.File
	sequence uint64
}

// Open creates (or reopens) the WAL at path for appending. Appends are
// written and fsync'ed before Append returns, which is what makes an
// acknowledged call durable.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &Log{f: f}, nil
}

// Append writes one record durably (write + fsync). The record's sequence
// must equal the last sequence + 1, which enforces a gapless ordering.
func (l *Log) Append(rec Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if rec.Sequence != l.sequence+1 {
		return ErrSequenceOutOfOrder
	}
	data := encode(rec)
	if _, err := l.f.Write(data); err != nil {
		return fmt.Errorf("wal: append: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	l.sequence = rec.Sequence
	return nil
}

// Seed adopts an externally recovered sequence number so that future appends
// continue the chain after a restart (the storage layer seeds this from the
// last replayed record).
func (l *Log) Seed(sequence uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sequence = sequence
}

// Sequence returns the highest sequence written to (or recovered for) this
// log. It is 0 for a fresh log.
func (l *Log) Sequence() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sequence
}

// NextSequence returns the sequence the next Append must use.
func (l *Log) NextSequence() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sequence + 1
}

// Replay reads all complete records from r in order.
//
// If the file ends in a partial record (a crash mid-write), the records
// before it are returned. When that happens the returned records are valid
// and are accompanied by ErrTornTail so the caller can decide to truncate.
//
// A record whose checksum fails is real corruption: an error is returned and
// no records after it are decoded.
func Replay(r io.Reader) ([]Record, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("wal: read: %w", err)
	}

	var records []Record
	pos := 0
	for {
		rec, next, err := decode(data, pos)
		if err == io.EOF {
			return records, nil
		}
		if err == ErrTornTail {
			return records, ErrTornTail
		}
		if err != nil {
			return records, err
		}
		records = append(records, rec)
		pos = next
	}
}

func encode(rec Record) []byte {
	buf := make([]byte, headerSize+len(rec.Key)+len(rec.Value))
	buf[0] = rec.Op
	binary.BigEndian.PutUint64(buf[1:9], rec.Sequence)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(rec.Key)))
	binary.BigEndian.PutUint32(buf[11:15], uint32(len(rec.Value)))
	copy(buf[headerSize:], rec.Key)
	copy(buf[headerSize+len(rec.Key):], rec.Value)
	sum := crc32.ChecksumIEEE(buf)
	buf = append(buf, make([]byte, 4)...)
	binary.BigEndian.PutUint32(buf[len(buf)-4:], sum)
	return buf
}

func decode(data []byte, pos int) (Record, int, error) {
	if len(data)-pos == 0 {
		return Record{}, 0, io.EOF
	}
	if len(data)-pos < headerSize {
		return Record{}, 0, ErrTornTail
	}
	op := data[pos]
	seq := binary.BigEndian.Uint64(data[pos+1 : pos+9])
	keyLen := int(binary.BigEndian.Uint16(data[pos+9 : pos+11]))
	valLen := int(binary.BigEndian.Uint32(data[pos+11 : pos+15]))

	payloadEnd := pos + headerSize + keyLen + valLen
	if len(data)-payloadEnd < 4 {
		return Record{}, 0, ErrTornTail
	}
	want := binary.BigEndian.Uint32(data[payloadEnd : payloadEnd+4])
	got := crc32.ChecksumIEEE(data[pos:payloadEnd])
	if want != got {
		return Record{}, 0, ErrCorrupt
	}
	rec := Record{
		Sequence: seq,
		Op:       op,
		Key:      string(data[pos+headerSize : pos+headerSize+keyLen]),
		Value:    string(data[pos+headerSize+keyLen : payloadEnd]),
	}
	return rec, payloadEnd + 4, nil
}

// Close releases the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		err := l.f.Close()
		l.f = nil
		return err
	}
	return nil
}