# Benchmarking KRaft

## Kafka Cluster Creation

1. run `create-kraft-broker.py -b <num brokers> -c <num controllers>`
NOTE: You do not have to specify both brokers and controllers. By only specifying the count of one, you will have a cluster of all combined role nodes (broker AND controller).
2. Use The go client to produce/consume from the cluster. 
3.  To change the kafka cluster settings: vim ./etc/kafka/kraft/server.properties
4. OR you can use The admin API to change your cluster settings. 
    
* #### By the end of this, you will have two docker images running on two ports: 9092 and 9094, representing a Kafka cluster that contains two brokers. 

* #### The producer logic is consuming the Github JSON file provided by Franco. 
   
   
