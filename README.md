# Benchmarking KRaft

## Kafka Cluster Creation

1. run `create-kraft-broker.py -b <num brokers> -c <num controllers>`

   NOTE: You do not have to specify both brokers and controllers. By only specifying the count of one, you will have a cluster of all combined role nodes (broker AND controller).

2. create-kraft-broker.py will output a list of bootstrap.servers (external ips you can connect to a kafka broker with)

   e.g. `localhost:58105,localhost:58107,localhost:58109,localhost:58111,localhost:58113`

3. run `pubsub-client/pubsub_client.go <bootstrap server list: single comma seperated arg> <datapath>`

   e.g. `pubsub-client/pubsub_client.go localhost:58105,localhost:58107,localhost:58109,localhost:58111,localhost:58113 datafile.json`

4.  To change the kafka cluster settings: vim ./etc/kafka/kraft/server.properties

5. OR you can use The admin API to change your cluster settings. 
    

* #### The producer logic is consuming the Github JSON file provided by Franco. 
   
   
