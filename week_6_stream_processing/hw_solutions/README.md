# HW 6

## Q1

* `Kafka Node is responsible to store topics` - Topics are stored in Kafka Node.
* `Zookeeper is removed form Kafka cluster starting from version 4.0` - Since Kafka 2.8 Zookeeper is deprecated.
* `Retention configuration ensures the messages not get lost over specific period of time.` - True
* `Group-Id ensures the messages are distributed to associated consumers.` - True

## Q2 
* `Topic Replication` - If Leader dies, we can listen the followers
* `Ack All` - Make sure that the message stored in Nodes

## Q3
* `Topic Partitioning` - Multiple consumers can listen to the same topic


## Q4
* `vendor_id` - We can split the partion by vendor ids

## Q5
* `Deserializer Configuration` - Producer need to know how to serialize not deserialize the message
* `Topics Subscription` - Producer published topics not subcribes.
* `Bootstrap Server` - Configurations 
* `Group-Id` - Produce need to know which partition the message belongs
* `Offset` - Producer need to know offset to send which message
* `Cluster Key and Cluster-Secret` - Cluster Creds