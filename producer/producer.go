package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092, localhost:9094"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)

	topic := "myTopic"
	//byteValue, er := ioutil.ReadAll(jsonFile)
	//if er != nil {
	//	fmt.Println(er)
	//}
	// Open our jsonFile
	file, err := os.Open("../data.json")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K

	for scanner.Scan() {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(scanner.Bytes()),
		}, nil)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Wait for message deliveries before shutting down
	p.Flush(100 * 1000)
}
