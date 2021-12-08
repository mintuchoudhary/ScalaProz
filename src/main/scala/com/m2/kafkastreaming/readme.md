Download kafka-zookeeper setup from : https://kafka.apache.org/downloads
Follow steps to setup zookeeper-kakfa locally:
https://www.goavega.com/install-apache-kafka-on-windows/

https://sparkbyexamples.com/spark/spark-streaming-with-kafka/.
https://customer.cloudkarafka.com/login

Kafka folder downloaded : D:\kafka

Zookeeper start:
cd bin/windows
zookeeper-server-start.bat ../../config/zookeeper.properties

Kafka Start:
cd bin/windows
kafka-server-start.bat ../../config/server.properties

Create topic:
bin/windows/kafka-console-producer.bat \ --broker-list localhost:9092 --topic json_topic

bin/windows/kafka-console-consumer.bat \ --broker-list localhost:9092 --topic josn_data_topic

Add dependency: 
<dependency>
     <groupId>org.apache.spark</groupId>
     <artifactId>spark-sql-kafka-0-10_2.11</artifactId>
     <version>2.4.0</version>
</dependency>