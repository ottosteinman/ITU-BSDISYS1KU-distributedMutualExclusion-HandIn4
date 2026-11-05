package main

import (
	"context"
	"log"
	"net"
	"os/signal"
	"syscall"
	"time"
)

// rounds is how many times each node tries to enter the critical section.
const rounds = 5

// table hardcodes the demo topology: each node's id, its listen address, and
// the peers it knows about. Adding a node is one more row here plus one more
// entry in every other row's peer list.
var table = []struct {
	id    string
	addr  string
	peers []peer
}{
	{
		id:    "node-1",
		addr:  "127.0.0.1:50051",
		peers: []peer{{id: "node-2", addr: "127.0.0.1:50052"}, {id: "node-3", addr: "127.0.0.1:50053"}},
	},
	{
		id:    "node-2",
		addr:  "127.0.0.1:50052",
		peers: []peer{{id: "node-1", addr: "127.0.0.1:50051"}, {id: "node-3", addr: "127.0.0.1:50053"}},
	},
	{
		id:    "node-3",
		addr:  "127.0.0.1:50053",
		peers: []peer{{id: "node-1", addr: "127.0.0.1:50051"}, {id: "node-2", addr: "127.0.0.1:50052"}},
	},
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1) Build the nodes and get every server listening first, so the
	//    workloads never dial a port that is not up yet.
	nodes := make([]*Node, 0, len(table))
	for _, cfg := range table {
		n, err := New(cfg.id, cfg.peers)
		if err != nil {
			log.Fatalf("%s: %v", cfg.id, err)
		}
		lis, err := net.Listen("tcp", cfg.addr)
		if err != nil {
			log.Fatalf("%s: %v", cfg.id, err)
		}
		go n.serve(lis)
		nodes = append(nodes, n)
	}
	time.Sleep(300 * time.Millisecond) // let the servers finish binding

	// 2) Start each node's workload: one goroutine per node, the simulated
	//    "process" that keeps wanting the critical section.
	done := make(chan struct{}, len(nodes))
	for _, n := range nodes {
		go func(n *Node) {
			n.Workload(ctx, rounds)
			done <- struct{}{}
		}(n)
	}
	for range nodes {
		<-done
	}

	// 3) Shut the servers down gracefully.
	for _, n := range nodes {
		n.Stop()
	}
}
