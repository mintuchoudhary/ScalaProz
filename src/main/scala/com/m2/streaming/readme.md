Download kafka-zookeeper setup from : https://kafka.apache.org/downloads
Follow steps to setup zookeeper-kakfa locally:
https://www.goavega.com/install-apache-kafka-on-windows/

https://sparkbyexamples.com/spark/spark-streaming-with-kafka/.
https://customer.cloudkarafka.com/login

Kafka folder downloaded : D:\kafka
Change log path in \config
1.  zookeeper.properties = dataDir=c:/tmp/zookeeper/
2.  server.properties = log.dirs=c:/tmp/kafka-logs
Zookeeper start:
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

Kafka Start:
.\bin\windows\kafka-server-start.bat .\config\server.properties

Create topic:
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic data_topic
Producer:
.\bin\windows\kafka-console-producer.bat \ --broker-list localhost:9092 --topic data_topic
Consumer
    Kafka version : > 2.0
.\bin\windows\kafka-console-consumer.bat \ --bootstrap-server localhost:9092 --topic data_topic

Add dependency: 
<dependency>
     <groupId>org.apache.spark</groupId>
     <artifactId>spark-sql-kafka-0-10_2.11</artifactId>
     <version>2.4.0</version>
</dependency>

**Output Mode**
Append mode: this is the default mode. Just the new rows are written to the sink.
Complete mode: it writes all the rows. It is supported just by groupBy or groupByKey aggregations.
Update mode: writes in the sink “only” all the rows that are updated. When there are no aggregations it works exactly the same as “append” mode

**Stream joining with static**
Join - Streaming joining with Static: right outer join, full outer join, right_semi not permitted
Join - Static joining with Streaming: left outer join, full outer join, left_semi not permitted
**Note = if streaming is on left then left join is allowed, else right**

Since 2.3 Spark, join = Stream vs Stream 
1. Only append mode supported in stream with stream join
2. inner join = YES
3. left / right outer joinns =YES But must have WATERMARKS
4. Full outer join = NO
````
//df1 = static dataframe
df2 = streaming dataframe
df1.join(d2,condition, joinType)