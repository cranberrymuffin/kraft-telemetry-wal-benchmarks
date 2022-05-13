package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	brokers := "localhost:61828,localhost:61830,localhost:61832,localhost:61834,localhost:61836"
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	defer func(c *kafka.Consumer) {
		err := c.Close()
		if err != nil {
			fmt.Printf("Error closing client: %s. Forcefully exiting", err)
			panic(err)
		}
	}(c)

	err = c.SubscribeTopics([]string{"1d3c03e7-05bd-407f-9d73-76c59a41fb2d", "^aRegex.*[Tt]opic"}, nil)
	if err != nil {
		return
	}

	for {
		msg, err := c.ReadMessage(time.Minute * 2)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			switch err.(kafka.Error).Code() {
			case kafka.ErrTimedOut:
				break
			default:
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}
	}
}
