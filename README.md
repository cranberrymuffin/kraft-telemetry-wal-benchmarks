# Benchmarking kraft or Kafka Raft 

## To run Kafka cluster 
1. Download these two images from the docker hub.
   1.1 https://hub.docker.com/r/latifah221b/brocker9092
   1.2 https://hub.docker.com/r/latifah221b/broker9094
   
2. After running the first image, the docker container will have a folder called "/Kafka" open it 
3. you will find a folder with the Kafka raft program "cd" to it, "confluent-7.0.1."

4. while you are on this folder "confluent-7.0.1.", follow this URL to start Kafka broker [1]:  https://developer.confluent.io/quickstart/kafka-local/
   ```
   4.1  ./bin/kafka-storage format \
                    --config ./etc/kafka/kraft/server.properties \
                    --cluster-id $(./bin/kafka-storage random-uuid)
                    
    4.5 Store the cluster id somewhere because you are going to need it to join the second broker with this broker. 

    4.6 start the broker by running ./bin/kafka-server-start ./etc/kafka/kraft/server.properties
    ```
5. Do the same steps to run the broker [2], but note that you will specify the same cluster-id to the second broker. 
6. To fetch the cluster id of your broker node, do this :

```
go run ./Admin.go localhost:9092 ----> it may take some time 
```
   
7. Use The go client to produce/consume from the cluster. 
8.  To change the kafka cluster settings: vim ./etc/kafka/kraft/server.properties
9. OR you can use The admin API to change your cluster settings. 
    
* #### By the end of this, you will have two docker images running on two ports: 9092 and 9094, representing a Kafka cluster that contains two brokers. 

* #### The producer logic is consuming the Github JSON file provided by Franco. 
   
   
