package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sonaji94/ASTRAKV/storage/memtable"
	"github.com/sonaji94/ASTRAKV/storage/wal"
)

const version = "0.4.0"

func main() {
	nodeID := flag.Int("node-id", 1, "node identifier")
	port := flag.Int("port", 5001, "listen port")
	dataDir := flag.String("data-dir", filepath.Join(".", "data"), "directory for the write-ahead log")
	memThreshold := flag.Int("memtable-size", 100000, "entries that seal the active MemTable")
	flag.Parse()

	fmt.Printf("AstraKV %s — node %d :%d (WAL + MemTable)\n", version, *nodeID, *port)

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}

	buffer := memtable.NewBuffer(*memThreshold)
	log, err := recoverFromWAL(filepath.Join(*dataDir, "wal.log"), buffer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}

	repl(buffer, log)
}

// recoverFromWAL replays the log into the MemTable buffer and returns a log
// positioned to continue the sequence chain.
func recoverFromWAL(path string, b *memtable.Buffer) (*wal.Log, error) {
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
				b.Put(rec.Key, rec.Value)
			case wal.OpDelete:
				b.Delete(rec.Key)
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

func repl(b *memtable.Buffer, log *wal.Log) {
	defer log.Close()
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}
		handle(handleArgs(scanner.Text()), b, log)
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
	default:
		fmt.Println("commands: PUT key value | GET key | DELETE key")
		return nil
	}
}

func handle(args []string, b *memtable.Buffer, log *wal.Log) {
	switch {
	case args == nil:
		return
	case args[0] == "PUT":
		if err := log.Append(wal.Record{Sequence: log.NextSequence(), Op: wal.OpPut, Key: args[1], Value: args[2]}); err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return
		}
		b.Put(args[1], args[2])
		fmt.Println("OK")
	case args[0] == "GET":
		value, status := b.Get(args[1])
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
		b.Delete(args[1])
		fmt.Println("OK")
	}
}