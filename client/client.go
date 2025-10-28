package main

import (
	proto "ITUServer/grpc"
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	conn, err := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Not working")
	}
	defer conn.Close()

	client := proto.NewChitChatClient(conn)
	stream, err := client.Chat(context.Background())
	if err != nil {
		log.Fatalf("Error creating stream: %v", err)
	}
	reader := bufio.NewReader(os.Stdin)

	joinMsg := &proto.ClientMessage{
		ClientId: "Me-id",
		Payload: &proto.ClientMessage_Join{
			Join: &proto.ClientJoin{Name: "Me"},
		},
	}
	if err := stream.Send(joinMsg); err != nil {
		log.Fatalf("Failed to send join: %v", err)
	}

	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				log.Println("Server closed the stream.")
				return
			}
			if err != nil {
				log.Fatalf("Failed to receive: %v", err)
				return
			}
			log.Printf("Received: %v", in)
		}
	}()

	for {
		fmt.Print("Enter message: ")
		text, _ := reader.ReadString('\n')
		if text == "exit\n" {
			message := &proto.ClientMessage{
				ClientId: "temp-id",
				Payload: &proto.ClientMessage_Leave{
					Leave: &proto.ClientLeave{},
				},
			}
			if err := stream.Send(message); err != nil {
				log.Fatalf("Failed to exit: %v", err)
			}
			break
		}

		//TODO mabey change this so i logs the attempt at a longer than allowed message?.
		if len(text) > 128 {
			fmt.Println("Message too long(max 128 characters).")
			continue
		}

		message := &proto.ClientMessage{
			ClientId: "temp-id",
			Payload: &proto.ClientMessage_Message{
				Message: &proto.ChatMessage{
					Text: text,
				},
			},
		}
		if err := stream.Send(message); err != nil {
			log.Fatalf("Failed to send message: %v", err)
			break
		}
	}
}
