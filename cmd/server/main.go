package main

import (
	"flag"
	"fmt"
)

const version = "0.1.0"

func main() {
	nodeID := flag.Int("node-id", 1, "node identifier")
	port := flag.Int("port", 5001, "listen port")
	flag.Parse()

	fmt.Printf("AstraKV %s — node %d listening on :%d (foundation build)\n", version, *nodeID, *port)
}