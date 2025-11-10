package main

import (
	proto "ITUServer/grpc"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeState int

const (
	RELEASED NodeState = iota
	WANTED
	HELD
)

type Node struct {
	proto.UnimplementedMutexServiceServer
	id             string
	port           string
	lamportClock   int64
	state          NodeState
	requestTime    int64
	peers          map[string]*PeerConnection
	pendingReplies int
	deferredQueue  []string
	mu             sync.Mutex
	replyChan      chan bool
	logFile        *os.File
}

type PeerConnection struct {
	id     string
	addr   string
	client proto.MutexServiceClient
	conn   *grpc.ClientConn
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func NewNode(id, port string, peerAddrs map[string]string) *Node {
	logFileName := fmt.Sprintf("node_%s.log", id)
	logFile, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	node := &Node{
		id:            id,
		port:          port,
		lamportClock:  0,
		state:         RELEASED,
		peers:         make(map[string]*PeerConnection),
		deferredQueue: make([]string, 0),
		replyChan:     make(chan bool, 100),
		logFile:       logFile,
	}

	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	// Connect to all peers
	for peerId, addr := range peerAddrs {
		if peerId != id {
			node.connectToPeer(peerId, addr)
		}
	}

	return node
}

func (n *Node) connectToPeer(peerId, addr string) {
	log.Printf("[Node:%s] [Clock:%d] [Discovery] Attempting to connect to peer %s at %s",
		n.id, n.lamportClock, peerId, addr)

	// Retry connection with backoff
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[Node:%s] [Clock:%d] [Discovery] Failed to connect to %s (attempt %d/%d): %v",
				n.id, n.lamportClock, peerId, i+1, maxRetries, err)
			time.Sleep(time.Second * time.Duration(i+1))
			continue
		}

		client := proto.NewMutexServiceClient(conn)
		n.peers[peerId] = &PeerConnection{
			id:     peerId,
			addr:   addr,
			client: client,
			conn:   conn,
		}
		log.Printf("[Node:%s] [Clock:%d] [Discovery] Successfully connected to peer %s",
			n.id, n.lamportClock, peerId)
		return
	}
	log.Printf("[Node:%s] [Clock:%d] [Discovery] Failed to connect to peer %s after %d attempts",
		n.id, n.lamportClock, peerId, maxRetries)
}

func (n *Node) Start() {
	listener, err := net.Listen("tcp", n.port)
	if err != nil {
		log.Fatalf("[Node:%s] [Clock:%d] [Startup] Failed to listen: %v", n.id, n.lamportClock, err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterMutexServiceServer(grpcServer, n)

	n.incrementClock()
	log.Printf("[Node:%s] [Clock:%d] [Startup] Node started on %s", n.id, n.lamportClock, n.port)
	fmt.Printf("[Node:%s] Started on %s\n", n.id, n.port)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("[Node:%s] [Clock:%d] [Startup] Failed to serve: %v", n.id, n.lamportClock, err)
		}
	}()

	// Simulate random critical section requests
	go n.simulateRequests()
}

func (n *Node) simulateRequests() {
	// Wait for all nodes to start
	time.Sleep(3 * time.Second)

	for i := 0; i < 3; i++ {
		// Random delay between requests
		delay := time.Duration(rand.Intn(5)+3) * time.Second
		time.Sleep(delay)

		n.RequestCriticalSection()
	}
}

func (n *Node) incrementClock() {
	n.mu.Lock()
	n.lamportClock++
	n.mu.Unlock()
}

func (n *Node) updateClock(receivedTime int64) {
	n.mu.Lock()
	n.lamportClock = max(n.lamportClock, receivedTime) + 1
	n.mu.Unlock()
}

func (n *Node) RequestCriticalSection() {
	n.mu.Lock()
	n.state = WANTED
	n.lamportClock++
	n.requestTime = n.lamportClock
	n.pendingReplies = len(n.peers)
	timestamp := n.requestTime
	n.mu.Unlock()

	log.Printf("[Node:%s] [Clock:%d] [Request] Requesting Critical Section at time %d",
		n.id, timestamp, timestamp)
	fmt.Printf("[Node:%s] [Clock:%d] Requesting Critical Section\n", n.id, timestamp)

	// Send request to all peers
	for _, peer := range n.peers {
		go n.sendRequest(peer, timestamp)
	}

	// Wait for all replies
	for i := 0; i < len(n.peers); i++ {
		<-n.replyChan
	}

	n.mu.Lock()
	n.state = HELD
	n.mu.Unlock()

	log.Printf("[Node:%s] [Clock:%d] [Enter] Entering Critical Section", n.id, n.lamportClock)
	fmt.Printf("[Node:%s] [Clock:%d] >>> ENTERING CRITICAL SECTION <<<\n", n.id, n.lamportClock)

	// Execute critical section
	n.executeCriticalSection()

	// Release critical section
	n.ReleaseCriticalSection()
}

func (n *Node) sendRequest(peer *PeerConnection, timestamp int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &proto.RequestMessage{
		NodeId:    n.id,
		Timestamp: timestamp,
	}

	log.Printf("[Node:%s] [Clock:%d] [Send] Sending REQUEST to %s with timestamp %d",
		n.id, timestamp, peer.id, timestamp)

	_, err := peer.client.Request(ctx, req)
	if err != nil {
		log.Printf("[Node:%s] [Clock:%d] [Error] Failed to send request to %s: %v",
			n.id, n.lamportClock, peer.id, err)
	}
}

func (n *Node) Request(ctx context.Context, req *proto.RequestMessage) (*proto.ReplyMessage, error) {
	n.updateClock(req.Timestamp)

	log.Printf("[Node:%s] [Clock:%d] [Receive] Received REQUEST from %s with timestamp %d",
		n.id, n.lamportClock, req.NodeId, req.Timestamp)

	n.mu.Lock()
	defer n.mu.Unlock()

	shouldDefer := false
	if n.state == HELD {
		shouldDefer = true
	} else if n.state == WANTED {
		// Compare timestamps, use node ID as tiebreaker
		if n.requestTime < req.Timestamp {
			shouldDefer = true
		} else if n.requestTime == req.Timestamp && n.id < req.NodeId {
			shouldDefer = true
		}
	}

	if shouldDefer {
		log.Printf("[Node:%s] [Clock:%d] [Defer] Deferring reply to %s (MyState:%v, MyTime:%d, TheirTime:%d)",
			n.id, n.lamportClock, req.NodeId, n.state, n.requestTime, req.Timestamp)
		n.deferredQueue = append(n.deferredQueue, req.NodeId)
		return &proto.ReplyMessage{NodeId: n.id}, nil
	}

	log.Printf("[Node:%s] [Clock:%d] [Send] Sending immediate REPLY to %s",
		n.id, n.lamportClock, req.NodeId)

	return &proto.ReplyMessage{NodeId: n.id}, nil
}

func (n *Node) Reply(ctx context.Context, reply *proto.ReplyMessage) (*proto.Ack, error) {
	n.incrementClock()

	log.Printf("[Node:%s] [Clock:%d] [Receive] Received REPLY from %s",
		n.id, n.lamportClock, reply.NodeId)

	n.mu.Lock()
	n.pendingReplies--
	n.mu.Unlock()

	n.replyChan <- true

	return &proto.Ack{}, nil
}

func (n *Node) executeCriticalSection() {
	// Simulate critical section work
	fmt.Printf("[Node:%s] [Clock:%d] *** CRITICAL SECTION: Writing to shared resource ***\n",
		n.id, n.lamportClock)
	log.Printf("[Node:%s] [Clock:%d] [CriticalSection] Executing critical operation",
		n.id, n.lamportClock)

	// Write to shared database/file
	sharedFile := "shared_database.txt"
	f, err := os.OpenFile(sharedFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[Node:%s] [Clock:%d] [Error] Failed to open shared database: %v",
			n.id, n.lamportClock, err)
		return
	}
	defer f.Close()

	entry := fmt.Sprintf("[%s] Node %s entered critical section at Lamport time %d\n",
		time.Now().Format("15:04:05.000"), n.id, n.lamportClock)
	f.WriteString(entry)

	// Simulate work
	time.Sleep(2 * time.Second)

	log.Printf("[Node:%s] [Clock:%d] [CriticalSection] Completed critical operation",
		n.id, n.lamportClock)
	fmt.Printf("[Node:%s] [Clock:%d] *** CRITICAL SECTION COMPLETE ***\n", n.id, n.lamportClock)
}

func (n *Node) ReleaseCriticalSection() {
	n.mu.Lock()
	n.state = RELEASED
	deferredCopy := make([]string, len(n.deferredQueue))
	copy(deferredCopy, n.deferredQueue)
	n.deferredQueue = make([]string, 0)
	n.mu.Unlock()

	n.incrementClock()
	log.Printf("[Node:%s] [Clock:%d] [Release] Released Critical Section, sending %d deferred replies",
		n.id, n.lamportClock, len(deferredCopy))
	fmt.Printf("[Node:%s] [Clock:%d] <<< RELEASED CRITICAL SECTION >>>\n", n.id, n.lamportClock)

	// Send deferred replies
	for _, peerId := range deferredCopy {
		if peer, ok := n.peers[peerId]; ok {
			go n.sendDeferredReply(peer)
		}
	}
}

func (n *Node) sendDeferredReply(peer *PeerConnection) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n.incrementClock()
	reply := &proto.ReplyMessage{NodeId: n.id}

	log.Printf("[Node:%s] [Clock:%d] [Send] Sending deferred REPLY to %s",
		n.id, n.lamportClock, peer.id)

	_, err := peer.client.Reply(ctx, reply)
	if err != nil {
		log.Printf("[Node:%s] [Clock:%d] [Error] Failed to send deferred reply to %s: %v",
			n.id, n.lamportClock, peer.id, err)
	}
}

func (n *Node) Close() {
	n.incrementClock()
	log.Printf("[Node:%s] [Clock:%d] [Shutdown] Node shutting down", n.id, n.lamportClock)

	for _, peer := range n.peers {
		peer.conn.Close()
	}

	if n.logFile != nil {
		n.logFile.Close()
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run node.go <node_id>")
		fmt.Println("Example: go run node.go 1")
		os.Exit(1)
	}

	nodeId := os.Args[1]

	// Hardcoded peer discovery (as per spec)
	allNodes := map[string]string{
		"1": "localhost:5051",
		"2": "localhost:5052",
		"3": "localhost:5053",
	}

	port := allNodes[nodeId]
	delete(allNodes, nodeId) // Remove self from peers

	node := NewNode(nodeId, port, allNodes)
	node.Start()

	fmt.Printf("Node %s running. Press Ctrl+C to stop.\n", nodeId)

	// Keep running
	select {}
}
