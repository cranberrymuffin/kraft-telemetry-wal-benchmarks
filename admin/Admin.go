package main

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr,
			"please specify broker  \n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]

	// Create a new AdminClient.
	// AdminClient can also be instantiated using an existing
	// Producer or Consumer instance, see NewAdminClientFromProducer and
	// NewAdminClientFromConsumer.
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	//ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()

	// Find Cluster Id
	ClusterID, err := a.ClusterID(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(ClusterID)
	// Set Admin options to wait for the operation to finish (or at most 60s)
	//maxDur, err := time.ParseDuration("60s")
	//if err != nil {
	//	panic("ParseDuration(60s)")
	//	}
	// Admin options
	//	kafka.SetAdminOperationTimeout(maxDur)

	a.Close()
}
