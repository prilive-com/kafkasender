package main

import (
	"context"
	"fmt"
	"log"
	"github.com/prilive-com/kafkasender/kafkasender"
)

func main() {
	config, err := kafkasender.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}
	
	producer, err := kafkasender.NewProducer(config, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	
	ctx := context.Background()
	err = producer.SendMessage(ctx, "tguser-registration", "test-key", "Hello from kafkasender!")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("Message sent successfully!")
	}
}