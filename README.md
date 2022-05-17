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

## Configuration Tuning for Write Throughput

Below we list the configuration variables for Brokers and Producers we intend to tune to improve write throughput.

### Broker Service

- Threads
  - num.io.threads
- compression.type
- min.insync.replicas
- Retention
  - log.retention.bytes
- log.retention.hours
- log.retention.minutes
- log.retention.ms
- replication.factor
  - default.replication.factor
  - offsets.topic.replication.factor

### Producer Client

- buffer.memory
- compression.type
- retries
- batch.size
- delivery.timeout.ms
- max.request.size
- partitioner.class (?)
- acks
- max.in.flight.requests.per.connection
