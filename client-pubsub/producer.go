// Example function-based Apache Kafka producer
package main

/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gofrs/uuid"
	"os"
)

func main() {
	brokers := "localhost:61828,localhost:61830,localhost:61832,localhost:61834,localhost:61836"
	uuidTopic, err := uuid.NewV4()
	topic := uuidTopic.String()
	if err != nil {
		fmt.Printf("Error creating topic uuid: %s\n", err)
		os.Exit(1)
	}
	println(topic)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	value := "Hello Go!"
	topicPartition := kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}
	//println(topicPartition.Partition)
	err = p.Produce(&kafka.Message{
		TopicPartition: topicPartition,
		Value:          []byte(value),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)
	if err != nil {
		fmt.Printf("Failed to produce message: %s\n", err)
		os.Exit(1)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
}
