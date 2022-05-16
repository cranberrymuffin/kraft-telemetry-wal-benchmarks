# Benchmarking Kraft (Zookeeper-less Kafka)

## Kafka Cluster Creation

1. Open the `cluster-scripts` folder

2. run `create-kraft-broker.py -b <num brokers> -c <num controllers>`

   NOTE: You do not have to specify both brokers and controllers. By only specifying the count of one, you will have a cluster of all combined role nodes (broker AND controller).

3. Step 2 will result in an output of bootstrap.servers (external ips you can connect to a kafka broker with) and generate a `kraft-docker-compose-autogen.yml` file in the same folder. It will also automatically set the bootstrap.servers for your producer client to connect to.
   
4. Run `docker-compose kraft-docker-compose-autogen.yml up -d` to start up the kafka cluster

## Producer Benchmarking
1. Open the `benchmark-scripts` folder

2. Run `benchmark-producer.py --data_file=<path to data file> --delimiter <optional arg for delimiters which aren't \n>`

3. Output will have benchmarks on write throughput in records/sec and mb/sec for the producer run on your cluster.

~~## Go Client Connection~~

~~3. Run `pubsub-client/pubsub_client.go <bootstrap server list: single comma seperated arg> <datapath>`. You will find the bootstrap server list as the output of step 2 from **Kafka Cluster Creation**~~

~~e.g. `pubsub-client/pubsub_client.go localhost:58105,localhost:58107,localhost:58109,localhost:58111,localhost:58113 datafile.json~~

~~4.  To change the kafka cluster settings: vim ./etc/kafka/kraft/server.properties~~

~~5. OR you can use The admin API to change your cluster settings.~~
    

* #### The producer logic is consuming the Github JSON file provided by Franco.
