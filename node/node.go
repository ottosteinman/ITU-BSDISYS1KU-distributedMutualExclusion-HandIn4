package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	mutex "MutualExclusion/gRPC"
	"MutualExclusion/logger"
)

// state is where a node stands in its own critical-section request.
type state int

const (
	stateIdle    state = iota // no request of our own in flight
	stateWaiting              // request sent, collecting replies
	stateInCS                 // inside the critical section
)

// peer is another node: the id it uses in messages, and where to reach it.
type peer struct {
	id   string
	addr string
}

// Node is one participant in the Ricart-Agrawala algorithm. It owns all
// per-node state and implements the generated gRPC server interface.
// One Node = one process in the textbook picture; running three Nodes in
// one binary over localhost simulates the distributed system.
type Node struct {
	mutex.UnimplementedRicart_ArgawalaServer // required for forward compatibility

	id      string
	clients map[string]mutex.Ricart_ArgawalaClient // keyed by peer id
	server  *grpc.Server

	// mu guards the protocol state below. gRPC handlers run in their own
	// goroutines, so every read/write of this state must hold the lock.
	mu         sync.Mutex
	clock      int64 // logical (Lamport-style) timestamp
	st         state
	myTS       int64                   // timestamp of the request we are waiting on
	repliesGot int                     // replies received for myTS
	gate       chan struct{}           // closed when repliesGot == len(clients)
	pending    []*mutex.RequestMessage // requests we deferred; granted on exit
}

// New builds a node and dials all of its peers.
func New(id string, peers []peer) (*Node, error) {
	n := &Node{id: id, clients: make(map[string]mutex.Ricart_ArgawalaClient, len(peers))}
	for _, p := range peers {
		conn, err := grpc.NewClient(p.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial %s (%s): %w", p.id, p.addr, err)
		}
		n.clients[p.id] = mutex.NewRicart_ArgawalaClient(conn)
	}
	return n, nil
}

// serve runs this node's gRPC server until Stop is called.
func (n *Node) serve(lis net.Listener) {
	n.server = grpc.NewServer()
	mutex.RegisterRicart_ArgawalaServer(n.server, n)
	logger.Log(n.id, "LISTEN", lis.Addr().String())
	if err := n.server.Serve(lis); err != nil {
		logger.Log(n.id, "SERVE_ERROR", err.Error())
	}
}

// Stop gracefully shuts the node's server down.
func (n *Node) Stop() {
	if n.server != nil {
		n.server.GracefulStop()
	}
}

// ---------------------------------------------------------------------------
// Ricart-Agrawala
// ---------------------------------------------------------------------------

// Request is the server side of "peer wants to enter the critical section".
//
// We grant (reply now) only when we will not be in the CS before them:
//
//	idle                       -> reply now
//	waiting, theirs ahead      -> reply now (we are blocked by them anyway)
//	waiting, ours ahead        -> defer until we exit the CS
//	in the CS                  -> defer until we exit the CS
//
// The "in the CS" case must defer unconditionally, even a request with an
// earlier timestamp: messages can arrive late, and granting a lower
// timestamp while we are still inside would let two nodes be in the CS.
//
// Priority is (timestamp, id), lexicographically smaller wins.
func (n *Node) Request(ctx context.Context, req *mutex.RequestMessage) (*emptypb.Empty, error) {
	replyNow := false

	n.mu.Lock()
	n.clock = max(n.clock, req.GetLogicalTime())
	switch n.st {
	case stateIdle:
		replyNow = true
	case stateWaiting:
		oursHigher := n.myTS < req.GetLogicalTime() ||
			(n.myTS == req.GetLogicalTime() && n.id < req.GetNodeId())
		if oursHigher {
			n.pending = append(n.pending, req)
		} else {
			replyNow = true
		}
	case stateInCS:
		n.pending = append(n.pending, req)
	}
	n.mu.Unlock()

	if replyNow {
		n.sendReply(req.GetLogicalTime(), req.GetNodeId())
	} else {
		logger.Log(n.id, "DEFER", fmt.Sprintf("requester=%s ts=%d", req.GetNodeId(), req.GetLogicalTime()))
	}
	return &emptypb.Empty{}, nil
}

// Reply is the server side of "you may enter". A reply is counted only if it
// matches the request we are waiting on (same timestamp, addressed to us);
// stale or foreign replies are dropped.
func (n *Node) Reply(ctx context.Context, rep *mutex.ReplyMessage) (*emptypb.Empty, error) {
	n.mu.Lock()
	n.clock = max(n.clock, rep.GetLogicalTime())
	if n.st == stateWaiting && rep.GetRequesterId() == n.id && rep.GetLogicalTime() == n.myTS {
		n.repliesGot++
		if n.repliesGot == len(n.clients) {
			close(n.gate)
		}
	}
	n.mu.Unlock()
	return &emptypb.Empty{}, nil
}

// EnterCS broadcasts the request, waits for every peer's reply, then enters.
// It returns the timestamp used for the request and whether entry succeeded.
func (n *Node) EnterCS() (int64, bool) {
	n.mu.Lock()
	if n.st != stateIdle {
		n.mu.Unlock()
		return 0, false
	}
	n.clock++
	n.myTS = n.clock
	n.st = stateWaiting
	n.repliesGot = 0
	n.gate = make(chan struct{})
	ts := n.myTS // snapshot: the fan-out goroutines must not re-read myTS
	n.mu.Unlock()

	// Fan the request out to every peer, one goroutine per peer.
	var wg sync.WaitGroup
	for id, cli := range n.clients {
		wg.Add(1)
		go func(id string, cli mutex.Ricart_ArgawalaClient) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := cli.Request(ctx, &mutex.RequestMessage{LogicalTime: ts, NodeId: n.id}); err != nil {
				logger.Log(n.id, "REQUEST_ERROR", fmt.Sprintf("peer=%s err=%v", id, err))
			}
		}(id, cli)
	}
	wg.Wait()
	logger.Log(n.id, "REQUEST", fmt.Sprintf("ts=%d peers=%d", ts, len(n.clients)))

	// Wait until every peer has replied (or give up on this round).
	select {
	case <-n.gate:
	case <-time.After(5 * time.Second):
		n.mu.Lock()
		got := n.repliesGot
		n.st = stateIdle // drop the round; any late replies will be ignored
		n.mu.Unlock()
		logger.Log(n.id, "TIMEOUT", fmt.Sprintf("ts=%d replies=%d/%d", ts, got, len(n.clients)))
		return ts, false
	}

	n.mu.Lock()
	n.st = stateInCS
	n.mu.Unlock()
	return ts, true
}

// ExitCS leaves the critical section and grants every deferred request.
func (n *Node) ExitCS() {
	n.mu.Lock()
	n.st = stateIdle
	deferred := n.pending
	n.pending = nil
	n.mu.Unlock()

	for _, req := range deferred {
		n.sendReply(req.GetLogicalTime(), req.GetNodeId())
	}
	logger.Log(n.id, "EXIT_CS", fmt.Sprintf("granted=%d", len(deferred)))
}

// Workload simulates the process that keeps wanting the critical section:
// think a random while, enter, do some "work", exit, repeat.
func (n *Node) Workload(ctx context.Context, rounds int) {
	for i := 1; i <= rounds; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Random think time so the nodes' requests overlap in time.
		time.Sleep(time.Duration(100+rand.Intn(500)) * time.Millisecond)

		ts, ok := n.EnterCS()
		if !ok {
			continue
		}
		logger.Log(n.id, "ENTER_CS", fmt.Sprintf("ts=%d", ts))
		time.Sleep(200 * time.Millisecond) // simulated critical-section work
		n.ExitCS()
	}
	logger.Log(n.id, "DONE", fmt.Sprintf("rounds=%d", rounds))
}

// sendReply grants permission to the named requester.
func (n *Node) sendReply(ts int64, requester string) {
	n.mu.Lock()
	cli, ok := n.clients[requester]
	n.mu.Unlock()
	if !ok {
		logger.Log(n.id, "REPLY_DROPPED", fmt.Sprintf("unknown peer=%s", requester))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// The reply echoes the request's timestamp so the requester can match it.
	if _, err := cli.Reply(ctx, &mutex.ReplyMessage{
		LogicalTime: ts,
		RequesterId: requester,
		ReplyerId:   n.id,
	}); err != nil {
		logger.Log(n.id, "REPLY_ERROR", fmt.Sprintf("peer=%s err=%v", requester, err))
		return
	}
	logger.Log(n.id, "REPLY", fmt.Sprintf("to=%s ts=%d", requester, ts))
}
