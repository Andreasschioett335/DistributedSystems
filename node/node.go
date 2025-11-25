package proto

import (
	"fmt"
	"log"
	"net"
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
	Duration  int
	StartTime int64
	topBid    int
	topBidder string
	Bids      map[string]int
}

func newNode(id string, address string, peers []string, isLeader bool, Duration int) *Node {
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
			Bids:      make(map[string]int),
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

	go n.sendHeartbeat

	go n.checkHeartbeat

	go n.checkAuctionOver

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
}

func main() {
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

}
