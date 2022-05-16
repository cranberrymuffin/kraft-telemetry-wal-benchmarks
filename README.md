# Benchmarking Kraft (Zookeeper-less Kafka)

## Kafka Cluster Creation

1. Open the `cluster-scripts` folder

3. run `create-kraft-broker.py -b <num brokers> -c <num controllers>`

   NOTE: You do not have to specify both brokers and controllers. By only specifying the count of one, you will have a cluster of all combined role nodes (broker AND controller).

3. Step 2 will result in an output of bootstrap.servers (external ips you can connect to a kafka broker with) and generate a `kraft-docker-compose-autogen.yml` file in the same folder. It will also automatically set the bootstrap.servers for your producer client to connect to.
   
4. Run `docker-compose kraft-docker-compose-autogen.yml up -d` to start up the kafka cluster

## Producer Benchmarking
1. Open the `benchmark-scripts` folder

2. Run `benchmark-producer.py --data_file=<path to data file> --delimiter <optional arg for delimiters which aren't \n>`

3. Output will have benchmarks on write throughput in records/sec and mb/sec for the producer run on your cluster.
