package main

import (
	"context"
	pb "example.com/vector-storage/api/vectorstore/v1"
	vectorstore "example.com/vector-storage/sdk/go"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	target := os.Getenv("RPC_TARGET")
	if target == "" {
		target = "127.0.0.1:18250"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := vectorstore.Dial(ctx, target, os.Getenv("API_TOKEN"), vectorstore.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	health, err := client.Health(ctx, &pb.Empty{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("namespace:", health.Namespace)
	page, err := client.List(ctx, &pb.ListRequest{Limit: 10})
	if err != nil {
		log.Fatal(err)
	}
	for _, record := range page.Records {
		fmt.Printf("id=%s revision=%d vector=%v\n", record.Id, record.Revision, record.Vector)
	}
	fmt.Println("next_cursor:", page.NextCursor)
}
