package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sonaji94/ASTRAKV/storage/memtable"
	"github.com/sonaji94/ASTRAKV/storage/sstable"
	"github.com/sonaji94/ASTRAKV/storage/wal"
)

const version = "0.5.0"

type db struct {
	dataDir string
	buffer  *memtable.Buffer
	readers []*sstable.Reader
}

func (d *db) get(key string) (string, memtable.Status) {
	// 1. MemTables (active, then sealed, newest first).
	if value, status := d.buffer.Get(key); status != memtable.NotFound {
		return value, status
	}
	// 2. SSTables, newest first. A Deleted entry in a newer table shadows any
	// older value; a Found entry shadows everything older.
	for i := len(d.readers) - 1; i >= 0; i-- {
		entry, ok, err := d.readers[i].Get(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sstable read error %s: %v\n", d.readers[i].Path(), err)
			continue
		}
		if ok {
			if entry.Deleted {
				return "", memtable.Deleted
			}
			return entry.Value, memtable.Found
		}
	}
	return "", memtable.NotFound
}

func (d *db) close() {
	for _, r := range d.readers {
		r.Close()
	}
}

func main() {
	nodeID := flag.Int("node-id", 1, "node identifier")
	port := flag.Int("port", 5001, "listen port")
	dataDir := flag.String("data-dir", filepath.Join(".", "data"), "directory for the WAL and SSTables")
	memThreshold := flag.Int("memtable-size", 100000, "entries that seal the active MemTable")
	flag.Parse()

	fmt.Printf("AstraKV %s — node %d :%d (WAL + MemTable + SSTable)\n", version, *nodeID, *port)

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}

	d := &db{dataDir: *dataDir, buffer: memtable.NewBuffer(*memThreshold)}
	log, err := recoverFromWAL(filepath.Join(*dataDir, "wal.log"), d)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
	defer d.close()
	defer log.Close()

	if err := loadTables(d); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
	if err := d.flushSealed(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}

	repl(d, log)
}

// recoverFromWAL replays the log into the MemTable buffer and returns a log
// positioned to continue the sequence chain.
func recoverFromWAL(path string, d *db) (*wal.Log, error) {
	log, err := wal.Open(path)
	if err != nil {
		return nil, err
	}

	r, err := os.Open(path)
	if err != nil {
		log.Close()
		return nil, err
	}
	defer r.Close()

	if records, err := wal.Replay(r); err == nil || err == wal.ErrTornTail {
		for _, rec := range records {
			switch rec.Op {
			case wal.OpPut:
				d.buffer.Put(rec.Key, rec.Value)
			case wal.OpDelete:
				d.buffer.Delete(rec.Key)
			}
		}
		if len(records) > 0 {
			log.Seed(records[len(records)-1].Sequence)
		}
		switch err {
		case wal.ErrTornTail:
			fmt.Printf("recovered %d record(s) from WAL; torn tail ignored\n", len(records))
		default:
			fmt.Printf("recovered %d record(s) from WAL (seq up to %d)\n", len(records), log.Sequence())
		}
	} else {
		log.Close()
		return nil, fmt.Errorf("wal corruption: %v", err)
	}
	return log, nil
}

// loadTables re-opens every SSTable on disk and appends it to current tables
// (newest by name order last).
func loadTables(d *db) error {
	paths, err := filepath.Glob(filepath.Join(d.dataDir, "*.sst"))
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, p := range paths {
		r, err := sstable.Open(p)
		if err != nil {
			return fmt.Errorf("open %s: %w", p, err)
		}
		d.readers = append(d.readers, r)
	}
	return nil
}

// flushSealed writes every sealed MemTable to a new SSTable, then drops the
// sealed tables from memory.
func (d *db) flushSealed() error {
	sealed := d.buffer.Sealed()
	if len(sealed) == 0 {
		return nil
	}
	n, err := nextTableNumber(d.dataDir)
	if err != nil {
		return err
	}
	for _, tbl := range sealed {
		path := filepath.Join(d.dataDir, fmt.Sprintf("%06d.sst", n))
		w, err := sstable.NewWriter(path, 4096)
		if err != nil {
			return err
		}
		it := tbl.NewIterator()
		count := 0
		for it.Valid() {
			if err := w.Add(sstable.Entry{
				Key:     it.Key(),
				Value:   it.Value(),
				Deleted: it.Status() == memtable.Deleted,
			}); err != nil {
				w.Close()
				return err
			}
			it.Next()
			count++
		}
		if err := w.Close(); err != nil {
			return err
		}
		r, err := sstable.Open(path)
		if err != nil {
			return err
		}
		d.readers = append(d.readers, r)
		fmt.Printf("flushed %d entr%s -> %s\n", count, plural(count), path)
		n++
	}
	d.buffer.Reset(sealed)
	return nil
}

func nextTableNumber(dataDir string) (int, error) {
	paths, err := filepath.Glob(filepath.Join(dataDir, "*.sst"))
	if err != nil {
		return 0, err
	}
	max := 0
	for _, p := range paths {
		base := filepath.Base(p)
		var num int
		if _, err := fmt.Sscanf(base, "%06d.sst", &num); err == nil && num > max {
			max = num
		}
	}
	return max + 1, nil
}

func plural(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

func repl(d *db, log *wal.Log) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}
		handle(handleArgs(scanner.Text()), d, log)
	}
}

func handleArgs(line string) []string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}
	cmd := strings.ToUpper(fields[0])
	switch cmd {
	case "PUT":
		if len(fields) < 3 {
			fmt.Println("usage: PUT key value")
			return nil
		}
		return []string{cmd, fields[1], strings.Join(fields[2:], " ")}
	case "GET", "DELETE":
		if len(fields) < 2 {
			fmt.Printf("usage: %s key\n", cmd)
			return nil
		}
		return []string{cmd, fields[1]}
	case "FLUSH":
		return []string{cmd}
	default:
		fmt.Println("commands: PUT key value | GET key | DELETE key | FLUSH")
		return nil
	}
}

func handle(args []string, d *db, log *wal.Log) {
	switch {
	case args == nil:
		return
	case args[0] == "PUT":
		if err := log.Append(wal.Record{Sequence: log.NextSequence(), Op: wal.OpPut, Key: args[1], Value: args[2]}); err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return
		}
		d.buffer.Put(args[1], args[2])
		fmt.Printf("OK (memtable %d entr%s)\n", d.buffer.Active().Entries(), plural(d.buffer.Active().Entries()))
	case args[0] == "GET":
		value, status := d.get(args[1])
		if status == memtable.Found {
			fmt.Println(value)
		} else {
			fmt.Println("NOT_FOUND")
		}
	case args[0] == "DELETE":
		if err := log.Append(wal.Record{Sequence: log.NextSequence(), Op: wal.OpDelete, Key: args[1]}); err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return
		}
		d.buffer.Delete(args[1])
		fmt.Printf("OK (memtable %d entr%s)\n", d.buffer.Active().Entries(), plural(d.buffer.Active().Entries()))
	case args[0] == "FLUSH":
		if err := d.flushSealed(); err != nil {
			fmt.Printf("ERROR: %v\n", err)
		} else {
			fmt.Printf("%d table(s) on disk\n", len(d.readers))
		}
	}
}