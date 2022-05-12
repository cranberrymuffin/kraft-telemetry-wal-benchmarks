# Benchmarking KRaft or Kafka Raft 

## Kafka Cluster Creation

1. run `create-kraft-broker.py -b <num brokers> -c <num controllers>`
NOTE: You do not have to specify both brokers and controllers. By only specifying the count of one, you will have a cluster of all combined role nodes (broker AND controller).


2. Use The go client to produce/consume from the cluster. 
3.  To change the kafka cluster settings: vim ./etc/kafka/kraft/server.properties
4. OR you can use The admin API to change your cluster settings. 
    

* #### The producer logic is consuming the Github JSON file provided by Franco. 
   
   
