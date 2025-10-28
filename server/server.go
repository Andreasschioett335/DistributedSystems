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

type ITU_databaseServer struct {
	proto.UnimplementedChitChatServer
	clients map[string]*client
	clock   int64
	idSeq   uint64
	conn    *grpc.Server
	mutex   sync.Mutex
}

type client struct {
	id   string
	name string
	send chan *proto.ServerEvent
}

type server struct {
	clients map[string]*client
	clock   int64
	idSeq   uint64
	conn    *grpc.Server
}

var nextID int64

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	server := &ITU_databaseServer{clients: make(map[string]*client)}
	server.start_server()
}

func (s *ITU_databaseServer) start_server() {
	listener, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("Failled to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterChitChatServer(grpcServer, s)
	log.Printf("[Server] [Startup] Server started on: %v", listener.Addr())
	defer log.Printf("[Server] [Shutdown] Server shutting down")
	err = grpcServer.Serve(listener)

	if err != nil {
		log.Fatalf("Failed to serve at: %v", err)
	}
}

func (s *ITU_databaseServer) addClient(c *client) {
	s.mutex.Lock()
	s.clients[c.id] = c
	s.mutex.Unlock()
	log.Printf("[Server] [Connect] [ClientID: %s]", c.id)
}

func (s *ITU_databaseServer) removeClient(id string) {
	s.mutex.Lock()
	if c, ok := s.clients[id]; ok {
		delete(s.clients, id)
		close(c.send)
		log.Printf("[Server] [Disconnect] [ClientID: %s] [Name: %s]", id, c.name)
	}
	s.mutex.Unlock()
}

func (s *ITU_databaseServer) broadcast(ev *proto.ServerEvent) {
	s.mutex.Lock()
	for _, c := range s.clients {
		select {
		case c.send <- ev:
		default:
			// drop if a client is too slow; keeps server simple
		}
	}
	s.mutex.Unlock()
}

func (s *ITU_databaseServer) Chat(stream proto.ChitChat_ChatServer) error {
	id := fmt.Sprintf("c-%d", atomic.AddInt64(&nextID, 1))
	name := "(anon)"

	c := &client{
		id:   id,
		name: name,
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

	s.broadcast(&proto.ServerEvent{
		EventType:      "system",
		Content:        fmt.Sprintf("%s connected", id),
		LogicalTime:    0,
		FromClientId:   id,
		FromClientName: name,
	})

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			// client closed sending side
			break
		}
		if err != nil {
			return err
		}

		switch pl := in.Payload.(type) {
		case *proto.ClientMessage_Join:
			name = pl.Join.Name
			s.broadcast(&proto.ServerEvent{
				EventType:      "join",
				Content:        fmt.Sprintf("%s joined", name),
				LogicalTime:    0,
				FromClientId:   id,
				FromClientName: name,
			})
			log.Printf("[Server] [Join] [ClientID: %s] [Name: %s]", id, name)

		case *proto.ClientMessage_Message:
			s.broadcast(&proto.ServerEvent{
				EventType:      "message",
				Content:        pl.Message.Text,
				LogicalTime:    0,
				FromClientId:   id,
				FromClientName: name,
			})
			log.Printf("[Server] [Message] [ClientID: %s] [Name: %s] %s", id, name, pl.Message.Text)

		case *proto.ClientMessage_Leave:
			// send a leave event and end the stream
			s.broadcast(&proto.ServerEvent{
				EventType:      "leave",
				Content:        pl.Leave.Leave,
				LogicalTime:    0,
				FromClientId:   id,
				FromClientName: name,
			})
			log.Printf("[Server] [Leave] [ClientID: %s] [Name: %s]", id, name)

			return nil

		default:
			// ignore unknown payloads
		}
	}
	// also drain sender end result (non-blocking)
	select {
	case <-sendErr:
	default:
	}
	return nil
}
