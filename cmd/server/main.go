package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sonaji94/ASTRAKV/storage/kvstore"
	"github.com/sonaji94/ASTRAKV/storage/wal"
)

const version = "0.3.0"

func main() {
	nodeID := flag.Int("node-id", 1, "node identifier")
	port := flag.Int("port", 5001, "listen port")
	dataDir := flag.String("data-dir", filepath.Join(".", "data"), "directory for the write-ahead log")
	flag.Parse()

	fmt.Printf("AstraKV %s — node %d :%d (WAL-backed)\n", version, *nodeID, *port)

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}

	store := kvstore.New()
	log, err := recoverFromWAL(filepath.Join(*dataDir, "wal.log"), store)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}

	repl(store, log)
}

// recoverFromWAL replays the log into the store and returns a log positioned
// to continue the sequence chain, implementing the WAL-before-memory rule on
// the way back in: only logged mutations participate in state.
func recoverFromWAL(path string, s *kvstore.Store) (*wal.Log, error) {
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
				s.Put(rec.Key, rec.Value)
			case wal.OpDelete:
				s.Delete(rec.Key)
			}
		}
		if len(records) > 0 {
			log.Seed(records[len(records)-1].Sequence)
		}
		if err == wal.ErrTornTail {
			fmt.Printf("recovered %d record(s) from WAL; torn tail ignored\n", len(records))
		} else {
			fmt.Printf("recovered %d record(s) from WAL (seq up to %d)\n", len(records), log.Sequence())
		}
	} else {
		log.Close()
		return nil, fmt.Errorf("wal corruption: %v", err)
	}
	return log, nil
}

func repl(s *kvstore.Store, log *wal.Log) {
	defer log.Close()
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}
		handle(handleArgs(scanner.Text()), s, log)
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

func handle(args []string, s *kvstore.Store, log *wal.Log) {
	switch {
	case args == nil:
		return
	case args[0] == "PUT":
		// WAL first, memory second: if logging fails the mutation is rejected.
		if err := log.Append(wal.Record{Sequence: log.NextSequence(), Op: wal.OpPut, Key: args[1], Value: args[2]}); err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return
		}
		s.Put(args[1], args[2])
		fmt.Println("OK")
	case args[0] == "GET":
		if value, ok := s.Get(args[1]); ok {
			fmt.Println(value)
		} else {
			fmt.Println("NOT_FOUND")
		}
	case args[0] == "DELETE":
		if err := log.Append(wal.Record{Sequence: log.NextSequence(), Op: wal.OpDelete, Key: args[1]}); err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return
		}
		if s.Delete(args[1]) {
			fmt.Println("OK")
		} else {
			fmt.Println("NOT_FOUND")
		}
	}
}