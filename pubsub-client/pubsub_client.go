package main

import (
	"bufio"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gofrs/uuid"
	"log"
	"math/rand"
	"os"
	"time"
)

type PubSubClient struct {
	adminClient *kafka.AdminClient
	subscriber  *kafka.Consumer
	publisher   *kafka.Producer
	topics      []string
}

func getFile(path string) *os.File {

	// Open our file
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	return file
}

func getRandomId() string {
	uuidObj, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	return uuidObj.String()
}

func NewPubSubClient(bootStrapServers string, numTopics int) (*PubSubClient, error) {
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

	publisher, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootStrapServers,
	})
	if err != nil {
		return nil, err
	}

	//TODO Create topics with admin client + chance type to kafka.TopicSpecification
	topics := make([]string, numTopics)
	for i, _ := range topics {
		topics[i] = getRandomId()
	}

	return &PubSubClient{
		adminClient: adminClient,
		subscriber:  subscriber,
		publisher:   publisher,
		topics:      topics,
	}, nil
}

func (p *PubSubClient) BatchPublishFrom(logFile string, batchSize int) {

}

func (p *PubSubClient) PublishFrom(logFile string) {
	file := getFile(logFile)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)
	fileScanner := bufio.NewScanner(file)

	for fileScanner.Scan() {
		err := p.publisher.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.topics[rand.Intn(len(p.topics))], Partition: kafka.PartitionAny},
			Value:          fileScanner.Bytes(),
		}, nil)
		if err != nil {
			panic(err)
		}
	}

	if err := fileScanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func (p *PubSubClient) Consume() {
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

func (p *PubSubClient) Shutdown() {
	p.publisher.Close()
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
	go pubSubClient.PublishFrom(os.Args[2])
	pubSubClient.Consume()
	pubSubClient.Shutdown()
}
