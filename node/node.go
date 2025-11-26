package proto

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
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
	return &Node{
		id:            id,
		address:       address,
		peers:         peers,
		isLeader:      isLeader,
		lastHeartbeat: make(map[string]int64),
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
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < len(n.peers); i++ {
		peerAddress := n.peers[i]
		conn, err := grpc.Dial(peerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
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
		var clients []proto.AuctionServiceClient
		for _, client := range n.peerClients {
			clients = append(clients, client)
		}
		n.clientMutex.RUnlock()

		for _, client := range clients {
			go func(c proto.AuctionServiceClient) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				req := &proto.HeartbeatRequest{
					NodeId:    n.id,
					Timestamp: time.Now().Unix(),
				}
				_, err := c.Heartbeat(ctx, req)
				if err != nil {
					log.Printf("failed to heartbeat: %v", err)
				}
			}(client)
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
		lastHB := n.lastHeartbeat[n.id]
		n.mu.RUnlock()

		currenttime := time.Now().Unix()
		if currenttime-lastHB > 5 {
			fmt.Printf("The leader is dead, long live the leader! ME: %s", n.id)
			n.isLeader = true
		}
	}
}

func (n *Node) deadLeaderElect() {
	n.clientMutex.RLock()
	var aliveNodes []string
	aliveNodes = append(aliveNodes, n.address) // add myself
	for peerAddr := range n.peerClients {
		aliveNodes = append(aliveNodes, peerAddr)
	}
	n.clientMutex.RUnlock()

	amILeader := true
	for i := 0; i < len(aliveNodes); i++ {
		if aliveNodes[i] < n.address {
			amILeader = false
			break
		}
	}

	n.isLeader = amILeader
	if n.isLeader {
		fmt.Printf("The leader is dead, LONG LIVE THE LEADER, ME: %s\n", n.id)
	} else {
		fmt.Printf("I (%s) bow the knee to our new leader", n.id)
	}
}

func (n *Node) checkAuctionOver() {
	ticker := time.NewTicker(time.Second)
	for {
		<-ticker.C
		n.stateMutex.RLock()
		currentTime := time.Now().Unix()
		elapsedTime := currentTime - n.state.StartTime
		if !n.state.isOver && elapsedTime >= n.state.Duration {
			n.state.isOver = true
			fmt.Printf("I (%s) declare this auction to be over!", n.id)
			if n.isLeader {
				go n.replicateToBackups()
			}
		}
		n.stateMutex.RUnlock()
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
	if !n.isLeader {
		return &proto.BidResponse{Outcome: "exception"}, nil
	}

	n.stateMutex.RLock()
	defer n.stateMutex.RUnlock()

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
	n.stateMutex.RLock()

	n.state.topBid = req.State.HighestBid
	n.state.topBidder = req.State.HighestBidder
	n.state.isOver = req.State.IsOver
	n.state.StartTime = req.State.StartTime
	n.state.Duration = req.State.Duration

	for bidder, amount := range n.state.Bids {
		n.state.Bids[bidder] = int32(amount)
	}

	n.stateMutex.Unlock()

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
		fmt.Println("Usage: go run node.go client bid <node-address> <bidder> <amount>")
		fmt.Println("Or go run node.go client result <node-address>")
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
			fmt.Println("Bids sent:", result)
		} else if command == "result" {
			if len(os.Args) != 4 {
				fmt.Println("Invalid command, reason: length")
				return
			}
			nodeAddress := os.Args[3]

			result, err := getResult(nodeAddress)
			if err != nil {
				fmt.Println("Error getting result:", err)
			}
			fmt.Println(result)
		}
		return
	}

	var peers []string
	var node1 = newNode("Node1", "localhost:5001", peers, true, 100)
	var node2 = newNode("Node2", "localhost:5002", peers, false, 100)
	var node3 = newNode("Node3", "localhost:5003", peers, false, 100)

	var peers1 []string
	peers1[0] = node2.address
	peers1[1] = node3.address
	node1.peers = peers1

	var peers2 []string
	peers2[0] = node1.address
	peers2[1] = node3.address
	node2.peers = peers2

	var peers3 []string
	peers3[0] = node1.address
	peers3[1] = node2.address
	node3.peers = peers3
	node1.startNode()
	node2.startNode()
	node3.startNode()

	select {}
}
