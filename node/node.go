package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	proto "distributedsystems/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	proto.UnimplementedAuctionServiceServer
	id            string
	address       string
	peers         []string
	isLeader      bool
	state         AuctionState
	stateMutex    sync.RWMutex
	clientMutex   sync.RWMutex
	conn          *net.UDPConn
	lastHeartbeat map[string]int64
	mu            sync.RWMutex
	grpcServer    *grpc.Server
	peerClients   map[string]proto.AuctionServiceClient
}

type AuctionState struct {
	isOver    bool
	Duration  int64
	StartTime int64
	topBid    int32
	topBidder string
	Bids      map[string]int32
}

func newNode(id string, address string, peers []string, isLeader bool, Duration int64) *Node {
	lastHeartbeat := make(map[string]int64)
	lastHeartbeat["leader"] = time.Now().Unix() // Initialize to current time

	return &Node{
		id:            id,
		address:       address,
		peers:         peers,
		isLeader:      isLeader,
		lastHeartbeat: lastHeartbeat,
		peerClients:   make(map[string]proto.AuctionServiceClient),
		mu:            sync.RWMutex{},
		state: AuctionState{
			isOver:    false,
			Duration:  Duration,
			StartTime: time.Now().Unix(),
			topBid:    0,
			topBidder: "",
			Bids:      make(map[string]int32),
		},
	}
}

func (n *Node) startNode() {
	//server
	listener, err := net.Listen("tcp", n.address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	n.grpcServer = grpc.NewServer()
	proto.RegisterAuctionServiceServer(n.grpcServer, n)

	fmt.Printf("node %s listening on %s\n", n.id, n.address)

	go n.connectPeers()

	go n.sendHeartbeat()

	go n.checkHeartbeat()

	go n.checkAuctionOver()

	go func() {
		err := n.grpcServer.Serve(listener)
		if err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
}

func (n *Node) connectPeers() {
	time.Sleep(2 * time.Second) // Give other nodes time to start

	for i := 0; i < len(n.peers); i++ {
		peerAddress := n.peers[i]

		// Retry connection a few times
		var conn *grpc.ClientConn
		var err error
		for retry := 0; retry < 5; retry++ {
			conn, err = grpc.Dial(peerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				break
			}
			fmt.Printf("node %s: failed to connect to %s, retrying... (%d/5)\n", n.id, peerAddress, retry+1)
			time.Sleep(1 * time.Second)
		}

		if err != nil {
			log.Printf("node %s: could not connect to peer %s: %v", n.id, peerAddress, err)
			continue
		}

		client := proto.NewAuctionServiceClient(conn)

		n.clientMutex.Lock()
		n.peerClients[peerAddress] = client
		n.clientMutex.Unlock()

		fmt.Printf("node %s connected to peer %s\n", n.id, peerAddress)
	}
}

func (n *Node) sendHeartbeat() {
	ticker := time.NewTicker(time.Second)
	for {
		<-ticker.C
		if !n.isLeader {
			continue
		}
		n.clientMutex.RLock()
		var clients []struct {
			client proto.AuctionServiceClient
			addr   string
		}
		for addr, client := range n.peerClients {
			clients = append(clients, struct {
				client proto.AuctionServiceClient
				addr   string
			}{client, addr})
		}
		n.clientMutex.RUnlock()

		for _, c := range clients {
			go func(client proto.AuctionServiceClient, addr string) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				req := &proto.HeartbeatRequest{
					NodeId:    n.id,
					Timestamp: time.Now().Unix(),
				}
				_, err := client.Heartbeat(ctx, req)
				if err != nil {
					log.Printf("Failed to send heartbeat to %s: %v. Removing from peer list.", addr, err)
					// Remove dead peer
					n.clientMutex.Lock()
					delete(n.peerClients, addr)
					n.clientMutex.Unlock()
				}
			}(c.client, c.addr)
		}
	}
}

func (n *Node) checkHeartbeat() {
	ticker := time.NewTicker(time.Second * 3)
	for {
		<-ticker.C
		if n.isLeader {
			continue
		}
		n.mu.RLock()
		lastHB := n.lastHeartbeat["leader"]
		n.mu.RUnlock()

		currenttime := time.Now().Unix()
		if lastHB > 0 && currenttime-lastHB > 10 {
			fmt.Printf("The leader is dead! Last heartbeat was %d seconds ago\n", currenttime-lastHB)
			go n.deadLeaderElect()
		}
	}
}

func (n *Node) deadLeaderElect() {
	n.mu.Lock()
	// Reset heartbeat to prevent repeated elections
	n.lastHeartbeat["leader"] = time.Now().Unix()
	n.mu.Unlock()

	// Find alive nodes by pinging them
	var aliveNodes []string
	aliveNodes = append(aliveNodes, n.address) // add myself

	n.clientMutex.RLock()
	peerAddresses := make([]string, 0, len(n.peerClients))
	for addr := range n.peerClients {
		peerAddresses = append(peerAddresses, addr)
	}
	n.clientMutex.RUnlock()

	// Check each peer to see if it's alive
	for _, peerAddr := range peerAddresses {
		conn, err := grpc.Dial(peerAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(500*time.Millisecond))

		if err == nil {
			conn.Close()
			aliveNodes = append(aliveNodes, peerAddr)
			fmt.Printf("Peer %s is alive\n", peerAddr)
		} else {
			fmt.Printf("Peer %s is dead, removing from consideration\n", peerAddr)
			// Remove dead peer from client list
			n.clientMutex.Lock()
			delete(n.peerClients, peerAddr)
			n.clientMutex.Unlock()
		}
	}

	// Elect leader based on lowest address among alive nodes
	amILeader := true
	for _, addr := range aliveNodes {
		if addr < n.address {
			amILeader = false
			break
		}
	}

	n.isLeader = amILeader
	if n.isLeader {
		fmt.Printf("The leader is dead, LONG LIVE THE LEADER, ME: %s\n", n.id)
		fmt.Printf("Alive nodes: %v\n", aliveNodes)
	} else {
		fmt.Printf("I (%s) bow the knee to our new leader\n", n.id)
		fmt.Printf("Alive nodes: %v\n", aliveNodes)
	}
}

func (n *Node) checkAuctionOver() {
	ticker := time.NewTicker(time.Second)
	for {
		<-ticker.C
		n.stateMutex.Lock() // Changed to Lock for write
		currentTime := time.Now().Unix()
		elapsedTime := currentTime - n.state.StartTime
		if !n.state.isOver && elapsedTime >= n.state.Duration {
			n.state.isOver = true
			fmt.Printf("I (%s) declare this auction to be over!\n", n.id)
			shouldReplicate := n.isLeader
			n.stateMutex.Unlock()
			if shouldReplicate {
				go n.replicateToBackups()
			}
		} else {
			n.stateMutex.Unlock()
		}
	}
}

func (n *Node) replicateToBackups() {
	n.stateMutex.RLock()

	copyOfState := &proto.AuctionState{
		Bidders:       make(map[string]bool),
		Bids:          make(map[string]int32),
		HighestBid:    n.state.topBid,
		HighestBidder: n.state.topBidder,
		StartTime:     n.state.StartTime,
		Duration:      n.state.Duration,
		IsOver:        n.state.isOver,
	}
	for bidder, amount := range n.state.Bids {
		copyOfState.Bids[bidder] = int32(amount)
		copyOfState.Bidders[bidder] = true
	}
	n.stateMutex.RUnlock()

	n.clientMutex.RLock()
	var clients []proto.AuctionServiceClient
	for _, client := range n.peerClients {
		clients = append(clients, client)
	}
	n.clientMutex.RUnlock()

	for i := range clients {
		go func(c proto.AuctionServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			req := &proto.ReplicateRequest{State: copyOfState}
			_, err := c.Replicate(ctx, req)
			if err != nil {
				log.Printf("failed to replicate: %v", err)
			}
		}(clients[i])
	}
}

func sendBid(nodeAddress string, bidder string, amount int32) (string, error) {
	conn, err := grpc.Dial(nodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "Exception", err
	}
	defer conn.Close()

	client := proto.NewAuctionServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &proto.BidRequest{
		Bidder: bidder,
		Amount: amount,
	}

	resp, err := client.Bid(ctx, req)
	if err != nil {
		return "Exception", err
	}

	return resp.Outcome, nil
}

func getResult(nodeAddress string) (string, error) {
	conn, err := grpc.Dial(nodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "Exception", err
	}
	defer conn.Close()

	client := proto.NewAuctionServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &proto.ResultRequest{}

	resp, err := client.Result(ctx, req)
	if err != nil {
		return "Exception", err
	}

	return resp.Message, nil
}

func (n *Node) Bid(ctx context.Context, req *proto.BidRequest) (*proto.BidResponse, error) {

	n.stateMutex.Lock() // Changed to Lock for write
	defer n.stateMutex.Unlock()

	if n.state.isOver {
		return &proto.BidResponse{Outcome: "exception"}, nil
	}

	bidderName := req.Bidder
	bidAmount := req.Amount

	_, exist := n.state.Bids[bidderName]
	if !exist {
		n.state.Bids[bidderName] = 0
	}

	previousBid := n.state.Bids[bidderName]
	if bidAmount <= previousBid {
		return &proto.BidResponse{Outcome: "fail"}, nil
	}

	if bidAmount <= n.state.topBid {
		return &proto.BidResponse{Outcome: "fail"}, nil
	}

	n.state.Bids[bidderName] = bidAmount
	n.state.topBid = bidAmount
	n.state.topBidder = bidderName

	fmt.Printf("node %s New bid: %s with bid at %d\n", n.id, bidderName, bidAmount)

	go n.replicateToBackups()

	return &proto.BidResponse{Outcome: "success"}, nil
}

func (n *Node) Result(ctx context.Context, req *proto.ResultRequest) (*proto.ResultResponse, error) {
	n.stateMutex.RLock()
	defer n.stateMutex.RUnlock()

	var msg string
	if n.state.isOver {
		if n.state.topBidder == "" {
			msg = "Auction is over, no one bid anything"
		} else {
			msg = fmt.Sprintf("Auction is over, %s won with a bid of %d", n.state.topBidder, n.state.topBid)
		}
	} else {
		if n.state.topBidder == "" {
			msg = "Auction is still going but no one has bid yet"
		} else {
			msg = fmt.Sprintf("Auction is still going, the highest bid is %d by %s", n.state.topBid, n.state.topBidder)
		}
	}
	return &proto.ResultResponse{Message: msg}, nil
}

func (n *Node) Replicate(ctx context.Context, req *proto.ReplicateRequest) (*proto.ReplicateResponse, error) {
	n.stateMutex.Lock() // Changed to Lock for write
	defer n.stateMutex.Unlock()

	n.state.topBid = req.State.HighestBid
	n.state.topBidder = req.State.HighestBidder
	n.state.isOver = req.State.IsOver
	n.state.StartTime = req.State.StartTime
	n.state.Duration = req.State.Duration

	for bidder, amount := range req.State.Bids {
		n.state.Bids[bidder] = int32(amount)
	}

	return &proto.ReplicateResponse{Success: true}, nil
}

func (n *Node) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	n.mu.Lock()
	n.lastHeartbeat["leader"] = req.Timestamp
	n.mu.Unlock()

	return &proto.HeartbeatResponse{}, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Invalid usage, check readme for correct use")
		return
	}

	if os.Args[1] == "client" {
		if len(os.Args) < 4 {
			fmt.Println("Invalid command, reason: length")
			return
		}
		command := os.Args[2]
		if command == "bid" {
			if len(os.Args) != 6 {
				fmt.Println("Invalid command, reason: length")
				return
			}
			nodeAddress := os.Args[3]
			bidder := os.Args[4]
			var amount int32
			fmt.Sscanf(os.Args[5], "%d", &amount)
			result, err := sendBid(nodeAddress, bidder, amount)
			if err != nil {
				fmt.Println("Error sending bid:", err)
				return
			}
			fmt.Println("Bid result:", result)
		} else if command == "result" {
			if len(os.Args) != 4 {
				fmt.Println("Invalid command, reason: length")
				return
			}
			nodeAddress := os.Args[3]

			result, err := getResult(nodeAddress)
			if err != nil {
				fmt.Println("Error getting result:", err)
				return
			}
			fmt.Println(result)
		}
		return
	}

	if os.Args[1] == "node" {
		if len(os.Args) < 4 {
			fmt.Println("Invalid command: need at least node-id, port, and one peer")
			return
		}

		nodeId := os.Args[2]
		port, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Println("Invalid port:", os.Args[3])
			return
		}

		// Build peer addresses from remaining arguments
		var peers []string
		for i := 4; i < len(os.Args); i++ {
			peerPort := os.Args[i]
			peers = append(peers, fmt.Sprintf("localhost:%s", peerPort))
		}

		address := fmt.Sprintf("localhost:%d", port)

		// Node 1 (lowest port) is the leader
		isLeader := (nodeId == "1")

		node := newNode(fmt.Sprintf("Node%s", nodeId), address, peers, isLeader, 100)

		fmt.Printf("Starting node %s on %s (leader: %v)\n", nodeId, address, isLeader)
		fmt.Printf("Peers: %v\n", peers)

		node.startNode()

		// Handle graceful shutdown
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		<-sigChan
		fmt.Println("\nShutting down gracefully...")
		node.grpcServer.GracefulStop()
		fmt.Println("Node stopped")
		return
	}

}
