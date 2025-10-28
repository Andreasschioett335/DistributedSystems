package main

import (
	proto "ITUServer/grpc"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
)

// Server implementation of ChitChat
type ITU_databaseServer struct {
	proto.UnimplementedChitChatServer
	clients map[string]*client
	mutex   sync.Mutex
}

type client struct {
	id   string
	name string
	send chan *proto.ServerEvent
}

var nextID int64

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	server := &ITU_databaseServer{clients: make(map[string]*client)}
	server.startServer()
}

// Start gRPC server
func (s *ITU_databaseServer) startServer() {
	listener, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("[Server] [StartupError] Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterChitChatServer(grpcServer, s)

	log.Printf("[Server] [Startup] Server started on %v", listener.Addr())
	defer log.Printf("[Server] [Shutdown] Server shutting down")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("[Server] [ServeError] %v", err)
	}
}

// Add client to list
func (s *ITU_databaseServer) addClient(c *client) {
	s.mutex.Lock()
	s.clients[c.id] = c
	s.mutex.Unlock()
	log.Printf("[Server] [Connect] [ClientID: %s] [Name: %s]", c.id, c.name)
}

// Remove client from list
func (s *ITU_databaseServer) removeClient(id string) {
	s.mutex.Lock()
	if c, ok := s.clients[id]; ok {
		delete(s.clients, id)
		close(c.send)
		log.Printf("[Server] [Disconnect] [ClientID: %s] [Name: %s]", c.id, c.name)
	}
	s.mutex.Unlock()
}

// Broadcast event to all clients
func (s *ITU_databaseServer) broadcast(ev *proto.ServerEvent) {
	s.mutex.Lock()
	for _, c := range s.clients {
		select {
		case c.send <- ev:
		default:
			// drop if client is slow
		}
	}
	s.mutex.Unlock()
}

// Handle client connection and message stream
func (s *ITU_databaseServer) Chat(stream proto.ChitChat_ChatServer) error {
	id := fmt.Sprintf("c-%d", atomic.AddInt64(&nextID, 1))

	c := &client{
		id:   id,
		name: "(anon)",
		send: make(chan *proto.ServerEvent, 32),
	}
	s.addClient(c)
	defer s.removeClient(id)

	sendErr := make(chan error, 1)
	go func() {
		for ev := range c.send {
			if err := stream.Send(ev); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()

	// Broadcast connection
	s.broadcast(&proto.ServerEvent{
		EventType:      "system",
		Content:        fmt.Sprintf("%s connected", c.id),
		LogicalTime:    0,
		FromClientId:   c.id,
		FromClientName: c.name,
	})
	log.Printf("[Server] [System] [ClientID: %s] [Name: %s] Connected", c.id, c.name)

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		switch pl := in.Payload.(type) {

		// When client joins
		case *proto.ClientMessage_Join:
			c.name = pl.Join.Name // ✅ update name globally
			s.broadcast(&proto.ServerEvent{
				EventType:      "join",
				Content:        fmt.Sprintf("%s joined ChitChat", c.name),
				LogicalTime:    0,
				FromClientId:   c.id,
				FromClientName: c.name,
			})
			//log.Printf("[Server] [Join] [ClientID: %s] [Name: %s] Joined ChitChat", c.id, c.name)

		// When client sends a chat message
		case *proto.ClientMessage_Message:
			s.broadcast(&proto.ServerEvent{
				EventType:      "message",
				Content:        pl.Message.Text,
				LogicalTime:    0,
				FromClientId:   c.id,
				FromClientName: c.name,
			})
			log.Printf("[Server] [Message] [ClientID: %s] [Name: %s] %s",
				c.id, c.name, pl.Message.Text)

		// When client leaves
		case *proto.ClientMessage_Leave:
			s.broadcast(&proto.ServerEvent{
				EventType:      "leave",
				Content:        fmt.Sprintf("%s left ChitChat", c.name),
				LogicalTime:    0,
				FromClientId:   c.id,
				FromClientName: c.name,
			})
			//log.Printf("[Server] [Leave] [ClientID: %s] [Name: %s]", c.id, c.name)
			return nil

		default:
			log.Printf("[Server] [UnknownPayload] [ClientID: %s] [Name: %s] Ignored unknown message type",
				c.id, c.name)
		}
	}

	select {
	case <-sendErr:
	default:
	}
	return nil
}
