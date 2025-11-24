package distributedsystems

import (
	"net"
	"sync"
	"time"
)

type Node struct {
	id            string
	address       string
	peers         []string
	isLeader      bool
	state         AuctionState
	stateMutex    sync.RWMutex
	conn          *net.UDPConn
	lastHeartbeat map[string]int64
	mu            sync.RWMutex
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
