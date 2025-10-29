package main

import (
	proto "ITUServer/grpc"
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func main() {
	var clock int64 = 0

	logFile, err := os.OpenFile("../server/server.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	conn, err := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := proto.NewChitChatClient(conn)
	stream, err := client.Chat(context.Background())
	if err != nil {
		log.Fatalf("Error creating stream: %v", err)
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter your name: ")
	nameRaw, _ := reader.ReadString('\n')
	name := strings.TrimSpace(nameRaw)
	if name == "" {
		name = "Anonymous"
	}

	clock++
	joinMsg := &proto.ClientMessage{
		ClientId:    "Me-id",
		LogicalTime: clock,
		Payload: &proto.ClientMessage_Join{
			Join: &proto.ClientJoin{Name: name},
		},
	}
	if err := stream.Send(joinMsg); err != nil {
		log.Fatalf("Failed to send join: %v", err)
	}
	log.Printf("[Client] [Clock:%d] [Join] [Name: %s]", clock, name)

	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				log.Printf("[Client] [Clock:%d] [Info] Server closed stream.", clock)
				return
			}
			if err != nil {
				log.Fatalf("Failed to receive: %v", err)
			}

			clock = max(clock, in.LogicalTime) + 1

			fmt.Printf("[%d] %s: %s\n", clock, in.FromClientName, in.Content)

			log.Printf("[Client] [Clock:%d] [Received] [From: %s] -> [Event: %s] %s",
				clock, in.FromClientName, in.EventType, in.Content)
		}
	}()

	for {
		fmt.Print("Enter message: ")
		textRaw, _ := reader.ReadString('\n')
		text := strings.TrimSpace(textRaw)

		if len(text) == 0 {
			continue
		}

		if text == "exit" {
			leaveMsg := &proto.ClientMessage{
				ClientId:    "temp-id",
				LogicalTime: clock,
				Payload: &proto.ClientMessage_Leave{
					Leave: &proto.ClientLeave{},
				},
			}
			if err := stream.Send(leaveMsg); err != nil {
				log.Fatalf("Failed to send leave: %v", err)
			}
			clock++
			log.Printf("[Client] [Clock:%d] [Leave] [Name: %s]", clock, name)
			break
		}

		msg := &proto.ClientMessage{
			ClientId:    "temp-id",
			LogicalTime: clock,
			Payload: &proto.ClientMessage_Message{
				Message: &proto.ChatMessage{Text: text},
			},
		}
		if err := stream.Send(msg); err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}

		clock++

		log.Printf("[Client] [Clock:%d] [Message] [Name: %s] Sent: %s",
			clock, name, text)
	}
}
