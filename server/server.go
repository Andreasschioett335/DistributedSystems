package main

import (
	proto "ITUServer/grpc"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
)

type ITU_databaseServer struct {
	proto.UnimplementedChitChatServer
	clients map[string]*client
	mutex   sync.Mutex
	clock   int64
}

type client struct {
	id   string
	name string
	send chan *proto.ServerEvent
}

var nextID int64

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func main() {
	logFile, err := os.OpenFile("server.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}
	defer logFile.Close()
	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	server := &ITU_databaseServer{clients: make(map[string]*client)}
	server.startServer()
}

func (s *ITU_databaseServer) startServer() {
	s.clock++
	listener, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("[Server] [Clock:%d] [StartupError] Failed to listen: %v", s.clock, err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterChitChatServer(grpcServer, s)

	log.Printf("[Server] [Clock:%d] [Startup] Server started on %v", s.clock, listener.Addr())
	defer log.Printf("[Server] [Clock:%d] [Shutdown] Server shutting down", s.clock)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("[Server] [Clock:%d] [ServeError] %v", s.clock, err)
	}
}

func (s *ITU_databaseServer) addClient(c *client) {
	s.mutex.Lock()
	s.clock++
	s.clients[c.id] = c
	s.mutex.Unlock()
	log.Printf("[Server] [Clock:%d] [Connect] [ClientID: %s] [Name: %s]", s.clock, c.id, c.name)
}

func (s *ITU_databaseServer) removeClient(id string) {
	s.mutex.Lock()
	s.clock++
	if c, ok := s.clients[id]; ok {
		delete(s.clients, id)
		close(c.send)
		log.Printf("[Server] [Clock:%d] [Disconnect] [ClientID: %s] [Name: %s]", s.clock, c.id, c.name)
	}
	s.mutex.Unlock()
}

func (s *ITU_databaseServer) broadcast(ev *proto.ServerEvent, senderName string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.clock++
	ev.LogicalTime = s.clock

	for _, c := range s.clients {
		select {
		case c.send <- ev:
			log.Printf("[Server] [Clock:%d] [Broadcast] [From: %s] -> [To: %s] [Event: %s] %s",
				s.clock, senderName, c.name, ev.EventType, ev.Content)
		default:
			log.Printf("[Server] [Clock:%d] [Warning] [BroadcastDropped] [From: %s] [To: %s]",
				s.clock, senderName, c.name)
		}
	}
}

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

	s.broadcast(&proto.ServerEvent{
		EventType:      "system",
		Content:        fmt.Sprintf("%s connected", c.id),
		LogicalTime:    s.clock,
		FromClientId:   c.id,
		FromClientName: c.name,
	}, c.name)

	log.Printf("[Server] [Clock:%d] [System] [ClientID: %s] [Name: %s] Connected",
		s.clock, c.id, c.name)

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		s.clock = max(s.clock, in.LogicalTime) + 1

		switch pl := in.Payload.(type) {

		case *proto.ClientMessage_Join:
			c.name = pl.Join.Name
			event := &proto.ServerEvent{
				EventType:      "join",
				Content:        fmt.Sprintf("%s joined ChitChat", c.name),
				LogicalTime:    s.clock,
				FromClientId:   c.id,
				FromClientName: c.name,
			}
			s.broadcast(event, c.name)
			log.Printf("[Server] [Clock:%d] [Join] [ClientID: %s] [Name: %s] Joined ChitChat",
				s.clock, c.id, c.name)

		case *proto.ClientMessage_Message:
			event := &proto.ServerEvent{
				EventType:      "message",
				Content:        pl.Message.Text,
				LogicalTime:    s.clock,
				FromClientId:   c.id,
				FromClientName: c.name,
			}
			s.broadcast(event, c.name)
			log.Printf("[Server] [Clock:%d] [Message] [ClientID: %s] [Name: %s] Sent: %s",
				s.clock, c.id, c.name, pl.Message.Text)

		case *proto.ClientMessage_Leave:
			event := &proto.ServerEvent{
				EventType:      "leave",
				Content:        fmt.Sprintf("%s left ChitChat", c.name),
				LogicalTime:    s.clock,
				FromClientId:   c.id,
				FromClientName: c.name,
			}
			s.broadcast(event, c.name)
			log.Printf("[Server] [Clock:%d] [Leave] [ClientID: %s] [Name: %s]",
				s.clock, c.id, c.name)
			return nil

		default:
			log.Printf("[Server] [Clock:%d] [UnknownPayload] [ClientID: %s] [Name: %s] Ignored unknown message type",
				s.clock, c.id, c.name)
		}
	}

	select {
	case <-sendErr:
	default:
	}
	return nil
}
