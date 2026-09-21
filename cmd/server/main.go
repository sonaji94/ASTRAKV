package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/sonajipawar/astrakv/storage/kvstore"
)

const version = "0.2.0"

func main() {
	nodeID := flag.Int("node-id", 1, "node identifier")
	port := flag.Int("port", 5001, "listen port")
	flag.Parse()

	fmt.Printf("AstraKV %s — node %d :%d (in-memory KV store)\n", version, *nodeID, *port)

	store := kvstore.New()
	repl(store)
}

func repl(s *kvstore.Store) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}
		handle(handleArgs(scanner.Text()), s)
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

func handle(args []string, s *kvstore.Store) {
	switch {
	case args == nil:
		return
	case args[0] == "PUT":
		s.Put(args[1], args[2])
		fmt.Println("OK")
	case args[0] == "GET":
		if value, ok := s.Get(args[1]); ok {
			fmt.Println(value)
		} else {
			fmt.Println("NOT_FOUND")
		}
	case args[0] == "DELETE":
		if s.Delete(args[1]) {
			fmt.Println("OK")
		} else {
			fmt.Println("NOT_FOUND")
		}
	}
}