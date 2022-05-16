package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gofrs/uuid"
	"os"
	"time"
)

type AdminClient struct {
	adminClient *kafka.AdminClient
	subscriber  *kafka.Consumer
	topics      []string
}

func getRandomId() string {
	uuidObj, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	return uuidObj.String()
}

func NewPubSubClient(bootStrapServers string, numTopics int) (*AdminClient, error) {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootStrapServers,
	})
	if err != nil {
		return nil, err
	}

	subscriber, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootStrapServers,
		"group.id":          getRandomId(),
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}

	//TODO change from hardcoded value to list (and some way to hook this list up with producer.config)
	topics := make([]string, 1)
	topics[0] = "GaoVh9wTS-Gykm5z2GEbPA"

	return &AdminClient{
		adminClient: adminClient,
		subscriber:  subscriber,
		topics:      topics,
	}, nil
}

func (p *AdminClient) Consume() {
	err := p.subscriber.SubscribeTopics(p.topics, nil)
	if err != nil {
		panic(err)
	}

	for {
		msg, err := p.subscriber.ReadMessage(3 * time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			switch err.(kafka.Error).Code() {
			case kafka.ErrTimedOut:
				break
			default:
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}
	}
}

func (p *AdminClient) Shutdown() {
	err := p.subscriber.Close()
	if err != nil {
		panic(err)
	}
	p.adminClient.Close()
}

func main() {
	bootstrapServers := os.Args[1]
	pubSubClient, err := NewPubSubClient(bootstrapServers, 5)
	if err != nil {
		panic(err)
	}
	//go pubSubClient.PublishFrom(os.Args[2])
	pubSubClient.Consume()
	pubSubClient.Shutdown()
}
